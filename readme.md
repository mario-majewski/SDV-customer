# Generate customer data with the SDV - Synthetic Data Vault

Proof of concept: using the [Synthetic Data Vault](https://www.sdv.dev/) to generate customer and address data 
based on the **csv** files from the **data** directory.

*** 

- To start this project you need to install SDV. Tested at the 1.2.1 version.
- Open **synth-generator.py** file in your favourite IDE and run it.
- It loads sample data files from the **data** folder
- New data are written in the **\[tableName]-new-data.csv** file

The model contains only one column association between person/company 
identity and sex columns as custom class.

***

### Known issues:

- The code requires improvement the custom constraint class - in the `fm_constraint.py` file.
