from dataclasses import dataclass
from datetime import datetime, tzinfo
import logging
import os.path as P
import time
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import sqlalchemy as sa


@dataclass
class CTDdataRecord():
    """information from ctd_data table"""
    basename: str
    filename: str
    lat: float
    lon: float
    cast_number: int
    site: str
    station: str
    # set to str when there is an error message
    date_first_in_pos: datetime | str

class OceanDB:
    def __init__(self, db_file, mdw_file, db_user, db_password, tz_mapping: dict[str, str] | None = None) -> None:
        self.db_file = db_file
        self.tz_mapping = tz_mapping or {}
        db_driver = r"{Microsoft Access Driver (*.mdb, *.accdb)}"
        if mdw_file is None:
            cnxn_str = (
                f"DRIVER={db_driver};"
                f"DBQ={db_file};"
                f"UID={db_user};"
                f"PWD={db_password};"
                f"READONLY=TRUE;"
                f"ExtendedAnsiSQL=1;"
            )
        else:
            cnxn_str = (
                f"DRIVER={db_driver};"
                f"DBQ={db_file};"
                f"SYSTEMDB={mdw_file};"
                f"UID={db_user};"
                f"PWD={db_password};"
                f"READONLY=TRUE;"
                f"ExtendedAnsiSQL=1;"
            )

        self.connection_url = sa.engine.URL.create("access+pyodbc", username=db_user, password=db_password, query={"odbc_connect": cnxn_str})

    def __load_tables(self):
        """load and store the tables needed for OceanDB methods"""

        init_start = time.time()
        engine = sa.engine.create_engine(self.connection_url)

        with engine.connect() as conn:
            query_start = time.time()
            logging.debug('OceanDB create and connect took %sms', query_start - init_start)

            self.field_trip = pd.read_sql('SELECT * FROM FieldTrip', conn, parse_dates=['DateStart', 'DateEnd'])
            self.sites = pd.read_sql('SELECT * FROM Sites', conn)
            self.deployment_data = pd.read_sql('SELECT * FROM DeploymentData', conn, parse_dates=['TimeDriftGPS', 'TimeFirstGoodData', 'TimeLastGoodData', 'TimeSwitchOff',
                                'TimeDriftInstrument', 'TimeFirstInPos', 'TimeLastInPos', 'TimeSwitchOn'
                                'TimeEstimatedRetrieval', 'TimeFirstWet', 'TimeOnDeck'])
            self.instruments = pd.read_sql('SELECT * FROM Instruments', conn)
            self.ctd_data = pd.read_sql('SELECT * FROM CTDData', conn)

            logging.info('loading tables from %s took %sms, ctd_data has %s files',
                         self.db_file, time.time() - query_start, len(self.ctd_data))

        engine.dispose()

    def lookup_latitude(self, base_file_name: str) -> float:
        """
        Get latitude for this file.
        @param base_file_name base hex filename (without .hex)
        @throws LookupError if not found
        """
        return self.get_ctd_data(base_file_name).lat

    def get_ctd_data(self, base_file_name: str, case_sensitive=False) -> CTDdataRecord:
        """"Get the ctd_data record for the file name
        @param base_file_name base hex filename (without .hex)
        @param case_sensitive false by default
        @throws LookupError if matched nothing/multiple
        @throws ValueError if base_file_name has file extension
        """
        if base_file_name.endswith(".hex"):
            raise ValueError("expected base file name, shouldn't have .hex extension")

        if not hasattr(self, "ctd_data"):
            self.__load_tables()

        ctd_deployment = pd.DataFrame()

        hex_filename = f'{base_file_name}.hex'
        match_on = f'hex filename "{hex_filename}"'

        regex_pattern = f'^{base_file_name}'
        ctd_deployment, match_column = self.__match_filename(regex_pattern, hex_filename, case_sensitive)

        if not ctd_deployment.empty:
            # hex filename match
            match_on = f'hex filename "{hex_filename}"'
            if len(ctd_deployment) > 1:
                raise LookupError(f'multiple ctd_data records match hex filename "{hex_filename}"')

        else:
            # no match on hex filename
            # may have been processed in the past so db filename includes processing steps appended
            # Example: FileName='WQN015CFACLWDB.cnv'

            # add C at the end of regex, otherwise could get false positives where one
            # filename is the prefix of another. For example: WQR10 would match WQR100
            # TODO what if base_file_name contains regex special characters?
            regex = f'^{base_file_name}C'
            # less confusing startswith text for logs and messages
            match_on = f'startswith "{regex[1:]}"'
            
            regex_pattern = f'^{base_file_name}C'
            ctd_data, match_column = self.__match_filename(regex_pattern, hex_filename, case_sensitive)
        
            if ctd_deployment.empty:
                # filename not in the db
                raise LookupError(f'no ctd_data record FileName {match_on}')
            elif len(ctd_deployment) > 1:
                raise LookupError(f'multiple ctd_data records FileName {match_on}')

        try:
            date_first_in_pos=self.__merge_datetime(ctd_deployment, 'DateFirstInPos', 'TimeFirstInPos', 'TimeZone')
        except Exception as e:
            date_first_in_pos=str(e)
            logging.exception('Unable to create FirstInPos datetime')

        rec = CTDdataRecord(
            basename=base_file_name,
            filename=ctd_deployment[match_column].values[0],
            lat=ctd_deployment['Latitude'].values[0],
            lon=ctd_deployment['Longitude'].values[0],
            cast_number=ctd_deployment['CastNumber'].values[0].item(),
            site=ctd_deployment['Site'].values[0],
            station=ctd_deployment['Station'].values[0],
            date_first_in_pos=date_first_in_pos
        )

        logging.info(f"OceanDB: found {match_on} in ctd_data latitude={rec.lat}, site={rec.site}, station={rec.station}")
        return rec

    def get_test_basename(self) -> tuple[str, float]:
        """Get a file basename and latitude for testing"""
        if not hasattr(self, "ctd_data"):
            self.__load_tables()

        ctd_data = self.ctd_data
        suffix = 'CFACLWDB.cnv'
        match = ctd_data[ctd_data['Linkfile1'].str.endswith(suffix, na=False)]
        if not match.empty:
            filename = match['Linkfile1'].values[0]
            lat = float(match['Latitude'].values[0])
            return filename[:-len(suffix)], lat
        else:
            raise LookupError('no file found for testing')

    def __merge_datetime(self, series: pd.DataFrame, date_col: str, time_col: str, tz_col: str) -> datetime:
        d: np.datetime64 = series[date_col].values[0]
        t: np.datetime64 = series[time_col].values[0]
        tz: str = series[tz_col].values[0].strip()

        d2 = pd.to_datetime(d)
        t2 = pd.to_datetime(t)

        try:
            if tz.startswith('UTC-') or tz.startswith('UTC+'):
                # values like UTC-31 or UTC+11
                offset = tz[3:]
                if len(offset) == 3:
                    # add 00 to match required HHMM format
                    offset = f'{offset}00'

                # tz values like "UTC-31" will cause this error:
                # ValueError: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not datetime.timedelta(days=-2, seconds=61200).
                # could potentially workaround this by subtracting days
                zoneinfo = datetime.strptime(offset, "%z").tzinfo
            else:
                zoneinfo = ZoneInfo(tz)
        except Exception as e:
            raise InvalidTimeZoneException(f'Error with timezone "{tz}"') from e

        try:
            if tz.startswith('UTC-') or tz.startswith('UTC+'):
                # values like UTC-31 or UTC+11
                offset = tz[3:]
                if len(offset) == 3:
                    # add 00 to match required HHMM format
                    offset = f'{offset}00'

                # tz values like "UTC-31" will cause this error:
                # ValueError: offset must be a timedelta strictly between -timedelta(hours=24) and timedelta(hours=24), not datetime.timedelta(days=-2, seconds=61200).
                # could potentially workaround this by subtracting days
                zoneinfo = datetime.strptime(offset, "%z").tzinfo
            else:
                zoneinfo = ZoneInfo(tz)
        except Exception as e:
            raise InvalidTimeZoneException(f'Error with timezone "{tz}"') from e

        dt = datetime(d2.year, d2.month, d2.day, t2.hour, t2.minute, t2.second, t2.microsecond, zoneinfo)
        #logging.debug('%s + %s (%s) = %s', d, t, tz, dt)
        return dt

    def __match_filename(self, regex_pattern, hex_filename, case_sensitive):
        
        ctd_data = self.ctd_data
        
        ctd_deployment = pd.DataFrame()
        match_column = ""
        match_on = ""
        
        ctd_deployment_in_Linkfile1 = ctd_data[
            ctd_data['Linkfile1'].str.contains(regex_pattern, case=case_sensitive, regex=True, na=False)]

        ctd_deployment_in_FileName = ctd_data[
            ctd_data['FileName'].str.contains(regex_pattern, case=case_sensitive, regex=True, na=False)]
            
        ctd_deployment = pd.DataFrame()
        match_column = ""
        if not ctd_deployment_in_Linkfile1.empty:
            # hex filename match
            match_on = f'hex filename "{hex_filename}"'
            match_column = "Linkfile1"
            if len(ctd_deployment_in_Linkfile1) > 1:
                raise LookupError(f'multiple ctd_data Linkfile1 records match hex filename "{hex_filename}"')
            ctd_deployment = ctd_deployment_in_Linkfile1.copy()
        elif not ctd_deployment_in_FileName.empty:
            # hex filename match
            match_on = f'hex filename "{hex_filename}"'
            match_column = "FileName"
            if len(ctd_deployment_in_FileName) > 1:
                raise LookupError(f'multiple ctd_data FileName records match hex filename "{hex_filename}"')
            ctd_deployment = ctd_deployment_in_FileName.copy()   
            
        return ctd_deployment, match_column

    def __map_tz(self, tz: str) -> str:
        """Map db timezone string to an IANA timezone string"""
        if tz in self.tz_mapping:
            tz = self.tz_mapping[tz]
        return tz
        
class InvalidTimeZoneException(ValueError):
    ...
    

