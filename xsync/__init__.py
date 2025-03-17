def demographics(js):
    result = dict()
    fields = [
        'gender',
        'handedness',
        'age',
        'yob',
        'weight',
        'ses',
        'race',
        'post_menstrual_age',
        'gestational_age',
        'ethnicity',
        'educationDesc',
        'education',
        'dob',
        'birth_weight'
    ]
    for name in fields:
        try:
            value = js['items'][0]['children'][0]['items'][0]['data_fields'][name]
            result[f'xnat:subjectData/demographics[@xsi:type=xnat:demographicData]/{name}'] = value
        except KeyError:
            pass
    return result
