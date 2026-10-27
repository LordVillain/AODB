text = "hello world hadoop spark big data Ilyinchyk Daniil hello world\n"
with open("input.txt", "w") as f:
    for _ in range(1700000):
        f.write(text)