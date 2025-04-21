"""
Setup script for the trading_system package.
"""

from setuptools import setup, find_packages

setup(
    name="trading_system",
    version="0.1.0",
    description="A trading system for algorithmic cryptocurrency trading",
    author="Trading Bot Team",
    author_email="example@example.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "numpy",
        "pandas",
        "matplotlib",
        "requests",
        "websocket-client",
        "python-dotenv",
        "influxdb-client",
        "aiohttp",
        "pydantic",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.9",
) 