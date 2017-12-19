import sys
import os
import random
from .utils import (
    cache_located_at,
    to_pickle,
    from_pickle,
)
import pandas as pd


class Storage:
    def __init__(self,
            split_key,
            num_split = 4,
            base_storage_path = '',
            storage_dir_name = '_storage',
            origin_file_name = 'origin.pickle',
            random_seed = None,
            force_disable_random_seed = False
        ):
        self.split_key = split_key
        self.num_split = num_split
        self.base_storage_path = base_storage_path
        self.storage_dir_name = storage_dir_name
        self.origin_file_name = origin_file_name
        self.storage_dir_path = os.path.join(base_storage_path, storage_dir_name)
        self.origin_dir = os.path.join(self.storage_dir_path, 'origin')
        self.meta_dir = os.path.join(self.storage_dir_path, 'meta')
        self.splited_dir = os.path.join(self.storage_dir_path, 'splited')
        self.result_dir = os.path.join(self.storage_dir_path, 'result')
        self.value_origin = None
        if random_seed is not None:
            random.seed(random_seed)
        else:
            if not force_disable_random_seed:
                random.seed(12345)

    def build(self):
        # create dirs
        os.makedirs(self.storage_dir_path, exist_ok = True)
        os.makedirs(self.origin_dir, exist_ok = True)
        os.makedirs(self.meta_dir, exist_ok = True)
        os.makedirs(self.splited_dir, exist_ok = True)
        os.makedirs(self.result_dir, exist_ok = True)

        return self

    def get_meta_file_name(self):
        return 'meta_' + self.origin_file_name

    def get_result_file_name(self):
        return 'result_' + self.origin_file_name

    def get_splited_file_name(self, splited_idx):
        return 'splited_{}_{}'.format(splited_idx, self.origin_file_name)

    def sync_origin(self, *args, **kwargs):
        self.value_origin = cache_located_at(os.path.join(self.origin_dir, self.origin_file_name))(self.load_origin)(*args, **kwargs)
        return self.value_origin

    def sync_meta(self):
        meta_file_path = os.path.join(self.meta_dir, self.get_meta_file_name())
        meta_df = from_pickle(meta_file_path)
        if meta_df is not None:
            return meta_df
        origin_df = self.value_origin
        if origin_df is None:
            raise ValueError('origin_df is not set.')
        if self.split_key not in origin_df.columns:
            raise Exception('invalid key')
        if self.num_split < 1:
            raise ValueError('num_split must be higher than 0.')
        unique_keys = origin_df[self.split_key].unique()
        if unique_keys.size < self.num_split:
            raise Exception('num key categories must be higher than or equal to num_split.')
    
        def _generate_idx(max_num):
            cnt = 0
            for i in range(self.num_split):
                cnt += 1
                yield i
            while True:
                if cnt == max_num:
                    raise StopIteration()
                cnt += 1
                yield random.randint(0, self.num_split - 1)
        _key_idx_map = {k: idx for k, idx in zip(unique_keys, _generate_idx(unique_keys.size))}
        meta_df = pd.DataFrame({
            self.split_key: origin_df[self.split_key],
            'splited_idx': origin_df[self.split_key].apply(lambda x: _key_idx_map[x])
        })
        to_pickle(meta_file_path, meta_df)
        return meta_df

    def sync_splited(self, splited_idx):
        df = self.value_origin
        if not isinstance(df, pd.DataFrame):
            raise TypeError('`df` must be pd.DataFrame')
        def _wrapped_func(*args, **kwargs):
            meta_df = from_pickle(os.path.join(self.meta_dir, self.get_meta_file_name()))
            if meta_df is None:
                raise Exception('meta_df is not found.')
            if not meta_df.splited_idx.unique().size == self.num_split:
                raise ValueError('invalid num_split')
            if not splited_idx < self.num_split:
                raise ValueError('invalid split_idx')
            valid_key_set = set(meta_df[meta_df.splited_idx == splited_idx][self.split_key])
            dfc = df[df[self.split_key].isin(valid_key_set)].copy()
            return self.transform(dfc, *args, **kwargs)
        return cache_located_at(os.path.join(self.splited_dir, self.get_splited_file_name(splited_idx)))(_wrapped_func)

    def sync_result(self):
        files = set(os.listdir(self.splited_dir))
        all_candidate_files = set(self.get_splited_file_name(i) for i in range(self.num_split))
        diff_files = all_candidate_files - files
        if not len(diff_files) == 0:
            raise Exception('some splited files does not exist ... diff: {}'.format(diff_files))
        result = pd.concat([from_pickle(os.path.join(self.splited_dir, name)) for name in list(all_candidate_files)])
        to_pickle(os.path.join(self.result_dir, self.get_result_file_name()), result)
        return result


    def load_origin(self):
        raise NotImplementedError()

    def transform(self, df, *args, **kwargs):
        raise NotImplementedError()
