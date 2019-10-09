from pathlib import Path
import pickle
import mmap

class SDB:
    def __init__(self, path, mode='r'):
        assert mode in {'r', 'w'}
        self.path = Path(path)
        self.mode = mode
        self.data_path = self.path / 'data.bin'
        self.idx_path = self.path / 'idx.pkl'
        if mode == 'w':
            self.path.mkdir(exist_ok=True)
            self.idx = {}
            self.data = self.data_path.open('wb')
            self.pos = 0
        elif mode == 'r':
            self.idx = pickle.load(self.idx_path.open('rb'))
            self.data = self.data_path.open('rb')
            self.mm = mmap.mmap(self.data.fileno(), 0, prot=mmap.PROT_READ)

    def __setitem__(self, key, value):
        assert self.mode == 'w'
        self.idx[key] = self.pos, len(value)
        self.data.write(value)
        self.pos += len(value)

    def __getitem__(self, key):
        assert self.mode == 'r'
        s, t = self.idx[key]
        return self.mm[s:s + t]

    def keys(self):
        return self.idx.keys()

    def close(self):
        if self.mode == 'w':
            pickle.dump(self.idx, (self.path / 'idx.pkl').open('wb'))
        self.data.close()

if __name__ == '__main__':
    db = SDB('foo', 'w')
    db['foo'] = b'foo value'
    db['foobar'] = b'foobar value'
    db.close()

    db = SDB('foo')
    assert(db['foobar'] == b'foobar value')
    assert(db['foo'] == b'foo value')
