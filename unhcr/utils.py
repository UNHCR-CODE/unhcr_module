"""
Overview
    This Python file (utils.py) provides a utility function filter_json designed to recursively remove a specific value (-0.999 by default) 
    from JSON-like data structures (dictionaries and lists). This is likely used for data cleaning or preprocessing, where the value 
    -0.999 represents missing or invalid data.

Key Components
    filter_json(obj, val=-0.999): 
        This function recursively traverses a given Python object (obj). If the object is a dictionary, 
        it creates a new dictionary containing only key-value pairs where the value is not equal to val. 
        If the object is a list, it creates a new list containing only items that are not equal to val. Otherwise, it returns the object unchanged. 
        The default value for val is -0.999. This function is crucial for cleaning JSON-like data by removing a specific placeholder value 
        representing missing or unwanted data.
 """
import pandas as pd

def filter_json(obj, val=-0.999):
    if isinstance(obj, dict):
        return {k: filter_json(v) for k, v in obj.items() if v != val}
    elif isinstance(obj, list):
        return [filter_json(item) for item in obj]
    else:
        return obj
##################
# Hey there - I've reviewed your changes - here's some feedback:

# Overall Comments:

# Remove unused pandas import
# Consider renaming function to 'filter_nested_dict' or similar since it operates on Python dicts/lists rather than JSON specifically
# Here's what I looked at during the review
# 🟡 General issues: 1 issue found
# 🟢 Security: all looks good
# 🟢 Testing: all looks good
# 🟢 Complexity: all looks good
# 🟢 Documentation: all looks good
# e:_UNHCR\CODE\unhcr_module\unhcr\utils.py:4

# issue(bug_risk): Inconsistent use of filter value in recursive filtering

# import pandas as pd

# def filter_json(obj, val=-0.999):
#     if isinstance(obj, dict):
#         return {k: filter_json(v) for k, v in obj.items() if v != val}
# The function should pass the val parameter in recursive calls to ensure consistent filtering across nested structures.


