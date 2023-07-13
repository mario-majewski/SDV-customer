import pandas as pd
import secrets
from sdv.constraints import create_custom_constraint_class

COMPANY_RATE = 90

# Constraint for the Female / Male sex - only for the Individual (PERS=0) person
# For Company (PERS=1) the sex column should be empty


def is_valid(column_names, data):
    pers_column = column_names[0]
    sex_column = column_names[1]
    is_valid_series = []

    # value 'C' for the company (PERS=1) is a fake value
    for index in data.index:
        if (data[pers_column][index] == 0 and data[sex_column][index] == 'F') \
                or (data[pers_column][index] == 0 and data[sex_column][index] == 'M'):
            is_valid_series.append(True)
        elif data[pers_column][index] == 1 and data[sex_column][index] == 'C':
            is_valid_series.append(True)
        else:
            is_valid_series.append(False)

    return pd.Series(is_valid_series)


def transform(column_names, data, custom_parameters):
    pers_column = column_names[0]
    sex_column = column_names[1]

    # for index in new_data.index:
    #     random_number = secrets.randbelow(100)
    #     if random_number > COMPANY_RATE:
    #         new_data[sex_column][index] = ''
    #         new_data[pers_column][index] = 1
    #     else:
    #         new_data[pers_column][index] = 0
    #         if secrets.randbelow(2) == 1:
    #             new_data[sex_column][index] = 'F'
    #         else:
    #             new_data[sex_column][index] = 'M'
    typical_value = data[pers_column].median()
    data[pers_column] = data[pers_column].mask(data[sex_column] == 'C', typical_value)

    # print("transformation data:")
    # print(data)

    return data


def reverse_transform(column_names, transformed_data, custom_parameters):
    pers_column = column_names[0]
    sex_column = column_names[1]

    transformed_data[pers_column] = transformed_data[pers_column].mask(transformed_data[sex_column] == 'C', 1)

    # print("reversed data:")
    # print(transformed_data)

    return transformed_data


FM_Constraint = create_custom_constraint_class(
    is_valid_fn=is_valid,
    transform_fn=transform,
    reverse_transform_fn=reverse_transform
)
