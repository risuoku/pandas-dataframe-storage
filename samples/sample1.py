from pds import Storage

import pandas as pd
import argparse


class SampleStorage(Storage):
    def load_origin(self):
        return pd.DataFrame({
            'hoge': [123, 456, 786, 333, 456, 123, 987, 3],
            'fuga': [876, 32, -123, 333, 159, 23, 97, 377],
            'poyo': [3, 3, 3, 1, 1, 0, 0, 2]
        })

    def transform(self, df, a, b):
        df['hoge'] = df.hoge * a
        df['fuga'] = df.fuga * b
        return df


def prepare(st):
    print('=== print origin')
    print(st.sync_origin())
    print('')
    print('=== print meta')
    print(st.sync_meta())
    print('')


def split(st, idx, *args, **kwargs):
    print('=== print origin')
    print(st.sync_origin())
    st.sync_meta()
    print('')
    print('=== print split .. idx: {}'.format(idx))
    print(st.sync_splited(int(idx))(*args))
    print('')


def merge(st):
    print('=== print origin')
    print(st.sync_origin())
    st.sync_meta()
    print('')
    print('=== print result')
    print(st.sync_result().sort_index())


if __name__ == '__main__':
    st = SampleStorage (
        'poyo',
        origin_file_name='sample1.pickle',
        num_split=2
    ).build()

    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', action='store', default='prepare')
    parser.add_argument('--idx', action='store', default=None)
    ns = parser.parse_args()

    if ns.mode == 'prepare':
        prepare(st)
    elif ns.mode == 'split':
        if ns.idx is None:
            raise ValueError('`idx` must be set in the split mode.')
        split(st, ns.idx, 2, 3)
    elif ns.mode == 'merge':
        merge(st)
    else:
        raise ValueError('invalid mode.')
