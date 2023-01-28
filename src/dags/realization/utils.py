import datetime
from json import JSONEncoder
from bson.objectid import ObjectId

class MyEncoder(JSONEncoder):
    """JSONEncoder for types."""
    def default(self, obj):
        # if isinstance(obj, datetime.date):
        #     return str(obj)

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, ObjectId):
            return str(obj)

        # if isinstance(obj, dict):
        #     data = {}
        #     for (k, v) in obj.items():
        #         data[k] = to_dict(v, classkey)
        #     return data
        # elif hasattr(obj, "_ast"):
        #     return to_dict(obj._ast())
        # elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        #     return [to_dict(v, classkey) for v in obj]
        # elif hasattr(obj, "__dict__"):
        #     data = dict([(key, to_dict(value, classkey))
        #                  for key, value in obj.__dict__.items()
        #                  if not callable(value) and not key.startswith('_')])
        #     if classkey is not None and hasattr(obj, "__class__"):
        #         data[classkey] = obj.__class__.__name__
        #     return data
        # else:
        #     return obj

        return JSONEncoder.default(self, obj)