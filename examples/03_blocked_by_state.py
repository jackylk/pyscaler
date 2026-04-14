"""
示例 3：无法直接分布式化的代码

这段代码有共享可变状态（全局 counter），xscale 会检测到
并告诉你需要重构。这是一个"分析会说 no"的案例。
"""
counter = {"total": 0}


def process(item):
    # 不好：依赖并修改全局状态，并行化会导致竞态
    counter["total"] += 1
    return item * 2


if __name__ == "__main__":
    results = [process(i) for i in range(100)]
    print(f"total = {counter['total']}")
