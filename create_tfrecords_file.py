import numpy as np
from StringIO import StringIO
import os.path
import numpy
import tensorflow as tf
 
def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))
 
 
def _bytes_feature(value):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))
 
def _float_feature(value):
    return tf.train.Feature(float_list=tf.train.FloatList(value=value))
 
 
def convert_to_byte(X, y, name):
    """Converts a dataset to tfrecords."""
#     f = numpy.load(numpy_file)
#     X = f['X']
#     y = f['y']
    if X.shape[0] != y.shape[0]:
        raise ValueError('X size %d does not match label size %d.' %
                         (X.shape[0], y.shape[0]))
    rows = X.shape[0]
    cols = X.shape[1]
    width = X.shape[2]
 
    filename = os.path.join( name + '.tfrecords')
    print('Writing', filename)
            
    with tf.python_io.TFRecordWriter(filename) as writer:
        for index in range(rows):
            X_raw = X[index].tostring()
            example = tf.train.Example(
              features=tf.train.Features(
                  feature={
 
                      'y': _int64_feature(int(y[index])),
                      'X': _bytes_feature(X_raw)
                  }))
            writer.write(example.SerializeToString())
 
 
def convert_to_float(X, y, name):
    """Converts a dataset to tfrecords."""
#     f = numpy.load(numpy_file)
#     X = f['X']
#     y = f['y']
    if X.shape[0] != y.shape[0]:
        raise ValueError('X size %d does not match label size %d.' %
                         (X.shape[0], y.shape[0]))
    rows = X.shape[0]
    cols = X.shape[1]
    width = X.shape[2]
 
    filename = os.path.join( name + '.tfrecords')
    print('Writing', filename)
            
    with tf.python_io.TFRecordWriter(filename) as writer:
        for index in range(rows):
            #X_raw = X[index].tostring()
            example = tf.train.Example(
              features=tf.train.Features(
                  feature={
 
                      'y': _int64_feature(int(y[index])),
                      'X': _float_feature(float(X[index]))
                  }))
            writer.write(example.SerializeToString())
 
 
def parse(filename_queue):
    reader = tf.TFRecordReader()
    _, serialized_example = reader.read(filename_queue)
    features = tf.parse_single_example(
      serialized_example,
      # Defaults are not specified since both keys are required.
      features={
          'X': tf.FixedLenFeature([], tf.string),
          'y': tf.FixedLenFeature([], tf.int64)
      })
 
    image = tf.decode_raw(features['X'], tf.float64)
    image = tf.cast(image, tf.float32)
    label = tf.cast(features['y'], tf.int32)
    return image, label
 
 
def input_fn(filenames, train, batch_size = batch_size, buffer_size=2048):
    
    with tf.name_scope('input'):
        filename_queue = tf.train.string_input_producer([filenames], num_epochs=num_epochs)
        image, label = parse(filename_queue)
   
    if t
        image = tf.reshape(image, [132])
        images, sparse_labels = tf.train.shuffle_batch(
            [image, label], batch_size=batch_size, num_threads=2,
            capacity=buffer_size,
           
            min_after_dequeue=1000) 
    else:
        
        num_repeat = 1
        image = tf.reshape(image, [132])
        images, sparse_labels = tf.train.shuffle_batch(
            [image, label], batch_size=batch_size, num_threads=2,
            capacity=buffer_size,
            min_after_dequeue=1000)
    
    x = {'x': images}
    y = sparse_labels
 
    return x , y
 
 
if __name__=='__main__':
 
    np_arr = np.load('sample_numpy.npz')
    X= np_arr['X']
    y = np_arr['y']
    convert_to_byte(X, y, 'tf_file')
