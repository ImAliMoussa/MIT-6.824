#!/bin/bash
grep -in "started new operation with id" 3a.txt > sending.txt
grep -in  "received new operation" 3a.txt > received.txt
python3 main.py
