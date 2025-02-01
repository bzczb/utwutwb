import ctypes


def id_from_obj(obj):
    # return int(np.uint64(id(obj)).astype(np.int64))
    return id(obj)


def obj_from_id(id_):
    return ctypes.cast(id_, ctypes.py_object).value
