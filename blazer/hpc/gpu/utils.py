# -*- coding: utf-8 -*-

"""
Outputs some information on CUDA-enabled devices on your computer,
including current memory usage.

It's a port of https://gist.github.com/f0k/0d6431e3faa60bffc788f8b4daa029b1
from C to Python with ctypes, so it can run without compiling anything. Note
that this is a direct translation with no attempt to make the code Pythonic.
It's meant as a general demonstration on how to obtain CUDA device information
from Python without resorting to nvidia-smi or a compiled Python extension.

Author: Jan Schlüter
License: MIT (https://gist.github.com/f0k/63a664160d016a491b2cbea15913d549#gistcomment-3870498)
"""
import ctypes
import logging
import sys

# Some constants taken from cuda.h
CUDA_SUCCESS = 0
CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT = 16
CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_MULTIPROCESSOR = 39
CU_DEVICE_ATTRIBUTE_CLOCK_RATE = 13
CU_DEVICE_ATTRIBUTE_MEMORY_CLOCK_RATE = 36


def ConvertSMVer2Cores(major, minor):
    # Returns the number of CUDA cores per multiprocessor for a given
    # Compute Capability version. There is no way to retrieve that via
    # the API, so it needs to be hard-coded.
    # See _ConvertSMVer2Cores in helper_cuda.h in NVIDIA's CUDA Samples.
    return {
        (1, 0): 8,  # Tesla
        (1, 1): 8,
        (1, 2): 8,
        (1, 3): 8,
        (2, 0): 32,  # Fermi
        (2, 1): 48,
        (3, 0): 192,  # Kepler
        (3, 2): 192,
        (3, 5): 192,
        (3, 7): 192,
        (5, 0): 128,  # Maxwell
        (5, 2): 128,
        (5, 3): 128,
        (6, 0): 64,  # Pascal
        (6, 1): 128,
        (6, 2): 128,
        (7, 0): 64,  # Volta
        (7, 2): 64,
        (7, 5): 64,  # Turing
        (8, 0): 64,  # Ampere
        (8, 6): 64,
    }.get((major, minor), 0)


def main():
    libnames = ("libcuda.so", "libcuda.dylib", "cuda.dll")
    for libname in libnames:
        try:
            cuda = ctypes.CDLL(libname)
        except OSError:
            continue
        else:
            break
    else:
        raise OSError("could not load any of: " + " ".join(libnames))

    nGpus = ctypes.c_int()
    name = b" " * 100
    cc_major = ctypes.c_int()
    cc_minor = ctypes.c_int()
    cores = ctypes.c_int()
    threads_per_core = ctypes.c_int()
    clockrate = ctypes.c_int()
    freeMem = ctypes.c_size_t()
    totalMem = ctypes.c_size_t()

    result = ctypes.c_int()
    device = ctypes.c_int()
    context = ctypes.c_void_p()
    error_str = ctypes.c_char_p()
    gpus = []

    result = cuda.cuInit(0)

    if result != CUDA_SUCCESS:
        cuda.cuGetErrorString(result, ctypes.byref(error_str))
        logging.debug(
            "cuInit failed with error code %d: %s" % (result, error_str.value.decode())
        )
        raise
    result = cuda.cuDeviceGetCount(ctypes.byref(nGpus))
    if result != CUDA_SUCCESS:
        cuda.cuGetErrorString(result, ctypes.byref(error_str))
        logging.debug(
            "cuDeviceGetCount failed with error code %d: %s"
            % (result, error_str.value.decode())
        )
        raise
    logging.debug("Found %d device(s)." % nGpus.value)
    for i in range(nGpus.value):
        gpu = {}
        result = cuda.cuDeviceGet(ctypes.byref(device), i)
        if result != CUDA_SUCCESS:
            cuda.cuGetErrorString(result, ctypes.byref(error_str))
            logging.debug(
                "cuDeviceGet failed with error code %d: %s"
                % (result, error_str.value.decode())
            )
            raise
        logging.debug("Device: %d" % i)
        gpu["id"] = i
        if (
            cuda.cuDeviceGetName(ctypes.c_char_p(name), len(name), device)
            == CUDA_SUCCESS
        ):
            logging.debug("  Name: %s" % (name.split(b"\0", 1)[0].decode()))
            gpu["name"] = name.split(b"\0", 1)[0].decode()
        if (
            cuda.cuDeviceComputeCapability(
                ctypes.byref(cc_major), ctypes.byref(cc_minor), device
            )
            == CUDA_SUCCESS
        ):
            logging.debug(
                "  Compute Capability: %d.%d" % (cc_major.value, cc_minor.value)
            )
        if (
            cuda.cuDeviceGetAttribute(
                ctypes.byref(cores), CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT, device
            )
            == CUDA_SUCCESS
        ):
            logging.debug("  Multiprocessors: %d" % cores.value)
            logging.debug(
                "  CUDA Cores: %s"
                % (
                    cores.value * ConvertSMVer2Cores(cc_major.value, cc_minor.value)
                    or "unknown"
                )
            )
            gpu["cores"] = (
                cores.value * ConvertSMVer2Cores(cc_major.value, cc_minor.value)
                or "unknown"
            )
            if (
                cuda.cuDeviceGetAttribute(
                    ctypes.byref(threads_per_core),
                    CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_MULTIPROCESSOR,
                    device,
                )
                == CUDA_SUCCESS
            ):
                logging.debug(
                    "  Concurrent threads: %d" % (cores.value * threads_per_core.value)
                )
        if (
            cuda.cuDeviceGetAttribute(
                ctypes.byref(clockrate), CU_DEVICE_ATTRIBUTE_CLOCK_RATE, device
            )
            == CUDA_SUCCESS
        ):
            logging.debug("  GPU clock: %g MHz" % (clockrate.value / 1000.0))
            gpu["clock"] = clockrate.value / 1000.0
        if (
            cuda.cuDeviceGetAttribute(
                ctypes.byref(clockrate), CU_DEVICE_ATTRIBUTE_MEMORY_CLOCK_RATE, device
            )
            == CUDA_SUCCESS
        ):
            logging.debug("  Memory clock: %g MHz" % (clockrate.value / 1000.0))
        try:
            result = cuda.cuCtxCreate_v2(ctypes.byref(context), 0, device)
        except AttributeError:
            result = cuda.cuCtxCreate(ctypes.byref(context), 0, device)
        if result != CUDA_SUCCESS:
            cuda.cuGetErrorString(result, ctypes.byref(error_str))
            logging.debug(
                "cuCtxCreate failed with error code %d: %s"
                % (result, error_str.value.decode())
            )
        else:
            try:
                result = cuda.cuMemGetInfo_v2(
                    ctypes.byref(freeMem), ctypes.byref(totalMem)
                )
            except AttributeError:
                result = cuda.cuMemGetInfo(
                    ctypes.byref(freeMem), ctypes.byref(totalMem)
                )
            if result == CUDA_SUCCESS:
                logging.debug("  Total Memory: %ld MiB" % (totalMem.value / 1024**2))
                gpu["memory"] = totalMem.value / 1024**2
                logging.debug("  Free Memory: %ld MiB" % (freeMem.value / 1024**2))
                gpu["free"] = freeMem.value / 1024**2
            else:
                cuda.cuGetErrorString(result, ctypes.byref(error_str))
                logging.debug(
                    "cuMemGetInfo failed with error code %d: %s"
                    % (result, error_str.value.decode())
                )
            cuda.cuCtxDetach(context)

            gpus += [gpu]

    return gpus


if __name__ == "__main__":
    print(main())
    sys.exit()
