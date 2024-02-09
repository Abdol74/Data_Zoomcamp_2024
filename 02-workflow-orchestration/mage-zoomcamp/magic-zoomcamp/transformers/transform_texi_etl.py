if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # print('Total taxi rides which passenger count is 0 equal {}'.format(data['passenger_count'].isin([0]).sum()))
    # print('Total taxi rides which trip distance is 0 equal {}'.format(data['trip_distance'].isin([0]).sum()))

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)] 

    print('BEFORE TRANSFORMATION OF SNAKE COLUMNS',data.columns)
    
    data.columns = data.columns \
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True) \
                .str.lower()
    print('AFTER TRANSFORMATION OF SNAKE COLUMNS',data.columns)
             
    # data.columns = data.columns.str.replace(' ','_').str.lower()
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    

    print('AFTER APPLIED TRANSOFORMATION THE SHAPE OF DATAFRAME IS {}'.format(data.shape))

    print('DISTINCT VALUES OF VENDOR_ID COLUMN IS {}'.format(data['vendor_id'].unique()))

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, 'dataframe has no column called vendor_id'
    assert (output['passenger_count'] > 0).all(), 'There are rides with passnger count more than 0'
    assert (output['trip_distance'] > 0).all(), 'There are rides with trip distance more than 0'
