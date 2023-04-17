def word_count():
    in_string = "Sheena loves apple Reena also loves apple and mangoes"

    word_dct = {}

    for word in in_string.split():
        word_dct.setdefault(word, 0)
        word_dct[word] = word_dct.get(word) + 1

    sorted_key = sorted(word_dct.items(), key=lambda tup: tup[1], reverse=True)

    print({key: value for key, value in sorted_key})


def list_to_dict():
    lst1_key = [i for i in range(5)]
    lst1_values = [i * i for i in range(5)]
    dic = {key: value for key, value in zip(lst1_key, lst1_values)}
    print(dic)
    print(dict(zip(lst1_key, lst1_values)))


def find_missing_num():
    a = [1, 2, 4, 5, 6, 7]
    n = a[-1]
    total = n * (n + 1)
    missing = (total // 2) - sum(a)
    print(missing)


def find_pair_sum(a,sum):
    a.sort()
    left = 0
    right = len(a)-1
    while left <= right:
        if a[left]+a[right] > sum :
            right = right-1
        elif a[left]+a[right] < sum :
            left = left+1
        elif a[left] + a[right] == sum:
            print(f"pair of {a[left]} & {a[right]} sum {sum}")
            left = left+1
            right = right-1



class Circle:

    version=".0.01"

    def __init__(self, radius):
        self.radius=radius
    @classmethod
    def to_string(cls):
        pass
    pass


if __name__ == "__main__":
    print("word count ---> demo")
    word_count()
    print("list to dict ---> demo")
    list_to_dict()
    print("find missing number ---> demo")
    find_missing_num()
    print("find pair of sum ---> demo")
    a = [3, 4, 5, 7, 8, 9, 19, 11, 10]
    sum= 17
    find_pair_sum(a,sum)

    print(vars(Circle(10)))
