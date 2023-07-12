from sdv.datasets.local import load_csvs
from sdv.metadata import MultiTableMetadata
from sdv.multi_table import HMASynthesizer
import os
import pandas as pd

# if trained model exist use it

# if not - load data and train the model


# folder contains many CSV files including customers, stblpers, stblcntry and stblcntry1
# SYNTHETIC_FOLDER = 'D:\\mozaic-synthetic-data\\'
SYNTHETIC_FOLDER = ''
DATA_FOLDER = f'{SYNTHETIC_FOLDER}data'
METADATA_FILE = 'cif_metadata.json'

datasets = load_csvs(folder_name=DATA_FOLDER)
customers_table = datasets['cif']
address_table = datasets['address']
pers_table = datasets['stblpers']

# create metadata for tables
metadata = MultiTableMetadata()

metadata.detect_table_from_dataframe(
    table_name='cif',
    data=customers_table
)

metadata.detect_table_from_dataframe(
    table_name='address',
    data=address_table
)

metadata.detect_table_from_dataframe(
    table_name='stblpers',
    data=pers_table
)

print("Metadata tables:")
print(metadata.tables)

# update information in tables
metadata.update_column(
    table_name='cif',
    column_name='ACN',
    sdtype='id',
    regex_format='[0-9]{12}'
)

metadata.update_column(
    table_name='cif',
    column_name='FNAME',
    sdtype='first_name'
)

metadata.update_column(
    table_name='cif',
    column_name='LNM',
    sdtype='last_name'
)

metadata.update_column(
    table_name='cif',
    column_name='TAXID',
    sdtype='numerical'
)

metadata.update_column(
    table_name='cif',
    column_name='MADDR',
    sdtype='id',
    # regex_format='[0-9]{1,12}'
)

metadata.update_column(
    table_name='cif',
    column_name='PADDR',
    sdtype='id',
    # regex_format='[0-9]{1,12}'
)

metadata.set_primary_key(
    table_name='cif',
    column_name='ACN'
    # column_name='MADDR'
)

metadata.update_column(
    table_name='cif',
    column_name='INC',
    sdtype='numerical'
)

metadata.update_column(
    table_name='cif',
    column_name='EMAIL',
    sdtype='email',
    pii=True
)

metadata.update_column(
    table_name='cif',
    column_name='PERS',
    sdtype='id'
)

metadata.update_column(
    table_name='cif',
    column_name='DOB',
    sdtype='datetime',
    datetime_format='%Y-%m-%d'
)

metadata.update_column(
    table_name='cif',
    column_name='SEX',
    sdtype='categorical',
)

# ADDRESS table
metadata.update_column(
    table_name='address',
    column_name='ID',
    sdtype='id',
    regex_format='[0-9]{1,12}'
)
metadata.set_primary_key(
    table_name='address',
    column_name='ID'
)

metadata.update_column(
    table_name='address',
    column_name='AD1',
    sdtype='street_name',
    pii=True
)

metadata.update_column(
    table_name='address',
    column_name='AD2',
    sdtype='building_number'
)

metadata.update_column(
    table_name='address',
    column_name='AD3',
    sdtype='numerical',
)

metadata.update_column(
    table_name='address',
    column_name='ZIP',
    sdtype='zipcode_in_state'
)

metadata.update_column(
    table_name='address',
    column_name='CITY',
    sdtype='city',
    pii=True
)

metadata.update_column(
    table_name='address',
    column_name='CNTRY',
    # sdtype='country_code'
    sdtype='categorical'
)
metadata.update_column(
    table_name='address',
    column_name='STATE',
    sdtype='state_abbr',
    pii=True
)

# STBLPERS
metadata.update_column(
    table_name='stblpers',
    column_name='PERS',
    sdtype='id'
)

metadata.set_primary_key(
    table_name='stblpers',
    column_name='PERS',
)

pers_constraint = {
    'constraint_class': 'ScalarRange',
    'table_name': 'stblpers',
    'constraint_parameters': {
        'column_name': 'PERS',
        'low_value': 0,
        'high_value': 1,
        'strict_boundaries': True
    }
}

print("Updated metadata:")
print(metadata.tables)

# table relations
metadata.add_relationship(
    parent_table_name='address',
    child_table_name='cif',
    parent_primary_key='ID',
    child_foreign_key='MADDR'
)
# metadata.add_relationship(
#     parent_table_name='address',
#     child_table_name='cif',
#     parent_primary_key='ID',
#     child_foreign_key='PADDR'
# )

metadata.add_relationship(
    parent_table_name='stblpers',
    child_table_name='cif',
    parent_primary_key='PERS',
    child_foreign_key='PERS'
)

# for test purposes only
metadata.validate()
# metadata.visualize(
#     show_table_details=True,
#     show_relationship_labels=True,
#     output_filepath='customers_metadata.png'
# )

if os.path.exists(METADATA_FILE) and os.path.isfile(METADATA_FILE):
    os.remove(METADATA_FILE)

metadata.save_to_json(filepath=METADATA_FILE)

# create HMA synthesizer
synthesizer = HMASynthesizer(metadata, locales='en_US')

synthesizer.auto_assign_transformers(datasets)
# synthesizer.add_constraints(
#     constraints=[
#         # pers_constraint # only numerical / datetime, not for ID !
#     ]
# )

print('Auto detected transformers:')
print(synthesizer.get_transformers(table_name='cif'))
print(synthesizer.get_transformers(table_name='address'))

# train the synthesizer
synthesizer.fit(datasets)

# save trained model
synthesizer.save('cif_synthesizer.pkl')

# and generate synthetic data
new_data = synthesizer.sample(scale=2)

print("Process finished with new (artificial) data:")
for table in new_data:
    data_frame = pd.DataFrame(new_data[table])
    print(data_frame)
