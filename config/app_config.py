"""Module for application specific configuration."""

EXPECTED_HEADERS = ['filename', 'timestamp', 'entry_id', 'temperature(c)', 'turbidity(ntu)',
    'dissolvedoxygen(g/ml)', 'ph', 'ammonia(g/ml)', 'nitrate(g/ml)',
    'population', 'fish_length(cm)', 'fish_weight(g)']

NUMERIC_HEADERS = ['temperature(c)', 'turbidity(ntu)', 'dissolvedoxygen(g/ml)', 'ph',
                   'ammonia(g/ml)', 'nitrate(g/ml)', 'fish_length(cm)', 'fish_weight(g)']

NON_NUMERIC_HEADERS = ['filename', 'timestamp', 'entry_id', 'date', 'missing headers']
