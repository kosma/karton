#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup

setup(name='cardboard',
      version='0.1a1',
      description='Redis reimplementation in Python',
      author='Kosma Moczek',
      author_email='kosma@kosma.pl',
      url='https://github.com/kosma/cardboard',
      packages=['cardboard'],
      scripts=['twisted_cardboard.py'],
      license='BSD',
     )
