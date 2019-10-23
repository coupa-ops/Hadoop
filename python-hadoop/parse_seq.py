import sys
import pdb
from hadoop.io import SequenceFile

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('usage: SequenceFileReader <filename> <output>')
    else:
        reader = SequenceFile.Reader(sys.argv[1], 4)


    key_class = reader.getKeyClass()
    value_class = reader.getValueClass()
    key = key_class()
    value = value_class()

    position = reader.getPosition()
    f = open(sys.argv[2],'w')
    while reader.next(key, value):
        print(value)
        f.write(value.toString()+'\n')
    reader.close()
    f.close()

