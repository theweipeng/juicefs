import os

from setuptools import setup, find_packages, Distribution
from wheel.bdist_wheel import bdist_wheel

# The following line will be replaced by the actual version number during the Make process
VERSION = "1.3.0"
BUILD_INFO = "BUILDDATE+COMMIT HASH"


class NativeDistribution(Distribution):
    def has_ext_modules(self):
        return True


class CtypesWheel(bdist_wheel):
    def get_tag(self):
        # ctypes 不绑定 CPython ABI，但 libjfs 必须使用实际构建平台标签。
        _, _, platform = super().get_tag()
        return "py3", "none", platform


setup(
    name='juicefs',
    version=os.environ.get('JUICEFS_BUILD_VERSION', VERSION),
    description=os.environ.get('JUICEFS_BUILD_INFO', BUILD_INFO),
    distclass=NativeDistribution,
    cmdclass={'bdist_wheel': CtypesWheel},
    package_data={'juicefs': ['*.so']},
    packages=find_packages(where=".", include=["juicefs", "juicefs.*"]),
    include_package_data=True,
    install_requires=['six'],
    entry_points={
        'fsspec.specs': [
            'jfs = juicefs.JuiceFS',
        ],
    },
)
