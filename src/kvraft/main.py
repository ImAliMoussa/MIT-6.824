#!/usr/bin/python3

def readMyInput(filename: str) -> list[str]:
    with open(filename) as f:
        return [line.rstrip() for line in f]

received = readMyInput("received.txt")
sent = readMyInput("sending.txt")

received = set([r.split(' ')[-1] for r in received])
sent = [r.split(' ')[-2] for r in sent]

print(len(sent), len(set(sent)), len(received), len(set(received)))
assert(len(sent) == len(set(sent)))

for s in sent:
    if s not in received:
        print(s)