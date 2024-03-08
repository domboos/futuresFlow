"""Misc Utility functions mainly for directory handling"""
import os

from dotenv import dotenv_values


def get_root_directory():
    """get Root Directory from env"""
    env_var = dotenv_values()
    root_dir = env_var['ROOT_DIRECTORY']
    if not isinstance(root_dir, str):
        raise KeyError('Define ROOT_DIRECTORY in your .env files,'
                       ' so that it points to ~/futures_flow')
    return env_var['ROOT_DIRECTORY']


def get_data_path():
    """
    Returns the path to the data directory.
    """
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")

if __name__ == '__main__':
    print(get_data_path())