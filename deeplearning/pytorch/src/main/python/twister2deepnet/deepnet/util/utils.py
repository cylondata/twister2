import numpy as np
from twister2deepnet.deepnet.data.DataUtil import DataUtil
from twister2.tset.TSet import TSet
from math import sqrt


def tset_to_numpy(tset: TSet):
    cache_tset = tset.cache()
    test_data_object = cache_tset.get_data()
    data_items = []
    for partition in test_data_object.get_partitions():
        for consumer in partition.consumer():
            data_items.append(consumer)
    return data_items


def _check_one_dim_array(arr: np.ndarray):
    return True if len(arr.shape) == 1 else False


def fix_array_shape(arr: np.ndarray):
    if _check_one_dim_array(arr):
        arr = np.reshape(arr, (arr.shape[0], 1))
    return arr


def format_data(data=None):
    """
    This method re-shapes the data to fit into the Network Input Shape
    :rtype: data re-formatted to fit to the network designed
    """
    data_shape = data.shape
    img_size = int(sqrt(data_shape[2]))
    data = np.reshape(data, (data_shape[0], data_shape[1], img_size, img_size))
    return data


def format_target(data=None):
    """
    Reshaping the mnist target values to fit into model
    :param data:
    :return:
    """
    data_shape = data.shape
    data = np.reshape(data, (data_shape[0], data_shape[1]))
    return data


def generate_minibatches(input_data=None, world_size=4, init_batch_size=128):
    """
    Specific For MNIST and 3 dimensional data
    This function is generated for MNIST only cannot be used in general for all data shapes
    :param input_data: data in numpy format with (N,M) format Here N number of samples
            M is the tensor length
    :return: For numpy we reshape this and return a tensor of the shape, (N, sqrt(M), sqrt(M))
    """
    bsz = int(init_batch_size / float(world_size))
    data = DataUtil.generate_minibatches(data=input_data, minibatch_size=bsz)
    return data
