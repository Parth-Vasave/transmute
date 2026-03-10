import os
import configparser
import sqlite3
import tomllib

import pandas as pd
import pyreadstat
import tomli_w
import yaml, json
from typing import Optional
from .converter_interface import ConverterInterface

class PandasConverter(ConverterInterface):
    supported_input_formats: set = {
        'csv',
        'xlsx',
        'json',
        'jsonl',
        'parquet',
        'yaml',
        'feather',
        'orc',
        'tsv',
        'xml',
        'html',
        'ods',
        'sqlite',
        'xls',     # read-only
        'dta',     # read-only
        'sav',     # read-only (SPSS)
        'xpt',     # read-only (SAS transport)
        'fwf',     # read-only (fixed-width)
        'toml',
        'ini',
        'env',
    }
    supported_output_formats: set = {
        'csv',
        'xlsx',
        'json',
        'jsonl',
        'parquet',
        'yaml',
        'feather',
        'orc',
        'tsv',
        'xml',
        'html',
        'ods',
        'sqlite',
        'toml',
        'ini',
        'env',
    }

    def __init__(self, input_file: str, output_dir: str, input_type: str, output_type: str):
        """
        Initialize Pandas converter.
        
        Args:
            input_file: Path to the input file
            output_dir: Directory where the output file will be saved
            input_type: Format of the input file (e.g., "csv", "xlsx")
            output_type: Format of the output file (e.g., "csv", "xlsx")
        """
        super().__init__(input_file, output_dir, input_type, output_type)
    
    def can_convert(self) -> bool:
        """
        Check if conversion between the specified formats is possible.
        
        Returns:
            True if conversion is possible, False otherwise
        """
        input_fmt = self.input_type.lower()
        output_fmt = self.output_type.lower()
        
        # Check if formats are supported
        if input_fmt not in self.supported_input_formats or output_fmt not in self.supported_output_formats:
            return False
        
        return True

    def convert(self, overwrite: bool = True, quality: Optional[str] = None) -> list[str]:
        """
        Convert the input file to the output format using Pandas.
        
        Args:
            overwrite: Whether to overwrite existing output file (default: True)
            quality: Not applicable for data formats, ignored
        
        Returns:
            List of paths to the converted output files.
        """
        if not self.can_convert():
            raise ValueError(f"Conversion from {self.input_type} to {self.output_type} is not supported.")
        
        # Prepare output file path
        base_name = os.path.splitext(os.path.basename(self.input_file))[0]
        output_file = os.path.join(self.output_dir, f"{base_name}.{self.output_type}")
        
        # Check for overwrite
        if os.path.exists(output_file) and not overwrite:
            raise FileExistsError(f"Output file {output_file} already exists and overwrite is set to False.")
        
        # Handle YAML <-> JSON <-> TOML conversions directly (preserve nested structure)
        if self.input_type in ['yaml', 'json', 'toml'] and self.output_type in ['yaml', 'json', 'toml']:
            if self.input_type == 'yaml':
                with open(self.input_file, 'r') as f:
                    data = yaml.safe_load(f)
            elif self.input_type == 'toml':
                with open(self.input_file, 'rb') as f:
                    data = tomllib.load(f)
            else:  # json
                with open(self.input_file, 'r') as f:
                    data = json.load(f)
            
            if self.output_type == 'yaml':
                with open(output_file, 'w') as f:
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            elif self.output_type == 'toml':
                with open(output_file, 'wb') as f:
                    tomli_w.dump(data, f)
            else:  # json
                with open(output_file, 'w') as f:
                    json.dump(data, f, indent=2)
            
            return [output_file]
        
        # For tabular conversions, use pandas
        df = None
        if self.input_type == 'csv':
            df = pd.read_csv(self.input_file)
        elif self.input_type == 'xlsx':
            df = pd.read_excel(self.input_file)
        elif self.input_type == 'json':
            with open(self.input_file, 'r') as f:
                data = json.load(f)
            # Try to convert to DataFrame - if it's a list of dicts, it works directly
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                # For nested structures, flatten them
                df = pd.json_normalize(data)
        elif self.input_type == 'parquet':
            df = pd.read_parquet(self.input_file)
        elif self.input_type == 'feather':
            df = pd.read_feather(self.input_file)
        elif self.input_type == 'orc':
            df = pd.read_orc(self.input_file)
        elif self.input_type == 'tsv':
            df = pd.read_csv(self.input_file, sep='\t')
        elif self.input_type == 'xml':
            df = pd.read_xml(self.input_file)
        elif self.input_type == 'html':
            tables = pd.read_html(self.input_file)
            df = tables[0]
        elif self.input_type == 'ods':
            df = pd.read_excel(self.input_file, engine='odf')
        elif self.input_type == 'xls':
            df = pd.read_excel(self.input_file, engine='xlrd')
        elif self.input_type == 'jsonl':
            df = pd.read_json(self.input_file, lines=True)
        elif self.input_type == 'sqlite':
            conn = sqlite3.connect(self.input_file)
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
            table_name = tables['name'].iloc[0]
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
            conn.close()
        elif self.input_type == 'dta':
            df = pd.read_stata(self.input_file)
        elif self.input_type == 'sav':
            df, _ = pyreadstat.read_sav(self.input_file)
        elif self.input_type == 'xpt':
            df, _ = pyreadstat.read_xport(self.input_file)
        elif self.input_type == 'fwf':
            df = pd.read_fwf(self.input_file)
        elif self.input_type == 'toml':
            with open(self.input_file, 'rb') as f:
                data = tomllib.load(f)
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.json_normalize(data)
        elif self.input_type == 'ini':
            config = configparser.ConfigParser()
            config.read(self.input_file)
            rows = []
            for section in config.sections():
                for key, value in config.items(section):
                    rows.append({'section': section, 'key': key, 'value': value})
            df = pd.DataFrame(rows, columns=['section', 'key', 'value'])
        elif self.input_type == 'env':
            rows = []
            with open(self.input_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    key, _, value = line.partition('=')
                    rows.append({'key': key.strip(), 'value': value.strip()})
            df = pd.DataFrame(rows, columns=['key', 'value'])
        elif self.input_type == 'yaml':
            with open(self.input_file, 'r') as f:
                data = yaml.safe_load(f)
            # Try to convert to DataFrame - if it's a list of dicts, it works directly
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                # For nested structures, flatten them
                df = pd.json_normalize(data)
        
        # Write DataFrame to output format
        if self.output_type == 'csv':
            df.to_csv(output_file, index=False)
        elif self.output_type == 'xlsx':
            df.to_excel(output_file, index=False)
        elif self.output_type == 'json':
            df.to_json(output_file, orient='records', indent=2)
        elif self.output_type == 'parquet':
            df.to_parquet(output_file, index=False)
        elif self.output_type == 'feather':
            df.to_feather(output_file)
        elif self.output_type == 'orc':
            df.to_orc(output_file, index=False)
        elif self.output_type == 'jsonl':
            df.to_json(output_file, orient='records', lines=True)
        elif self.output_type == 'sqlite':
            conn = sqlite3.connect(output_file)
            df.to_sql('data', conn, index=False, if_exists='replace')
            conn.close()
        elif self.output_type == 'tsv':
            df.to_csv(output_file, sep='\t', index=False)
        elif self.output_type == 'xml':
            df.to_xml(output_file, index=False)
        elif self.output_type == 'html':
            from lxml import etree
            html_str = df.to_html(index=False)
            root = etree.fromstring(html_str.encode(), etree.HTMLParser())
            with open(output_file, 'wb') as f:
                f.write(etree.tostring(root, pretty_print=True, method='html'))
        elif self.output_type == 'ods':
            df.to_excel(output_file, engine='odf', index=False)
        elif self.output_type == 'yaml':
            with open(output_file, 'w') as f:
                yaml.dump(df.to_dict(orient='records'), f, default_flow_style=False)
        elif self.output_type == 'toml':
            with open(output_file, 'wb') as f:
                tomli_w.dump({'data': df.to_dict(orient='records')}, f)
        elif self.output_type == 'ini':
            config = configparser.ConfigParser()
            if 'section' in df.columns and 'key' in df.columns and 'value' in df.columns:
                for _, row in df.iterrows():
                    section = str(row['section'])
                    if not config.has_section(section):
                        config.add_section(section)
                    config.set(section, str(row['key']), str(row['value']))
            else:
                config.add_section('data')
                for col in df.columns:
                    for i, val in enumerate(df[col]):
                        config.set('data', f'{col}_{i}', str(val))
            with open(output_file, 'w') as f:
                config.write(f)
        elif self.output_type == 'env':
            with open(output_file, 'w') as f:
                if 'key' in df.columns and 'value' in df.columns:
                    for _, row in df.iterrows():
                        f.write(f"{row['key']}={row['value']}\n")
                else:
                    for col in df.columns:
                        for i, val in enumerate(df[col]):
                            f.write(f"{col}_{i}={val}\n")

        return [output_file]