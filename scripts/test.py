import random
import string

# parameters
filename = "large.in"
n_symbols = 500
n_threads = 40
n_lines = 50000

std_dev = 100
min_price = 1000
max_price = 3000
min_count = 10
max_count = 30

# data
counter = 100
current_order_id = []
symbols = []
prices = []


def generate_cancel():
    if len(current_order_id) == 0:
        return ""

    (thread, number) = random.sample(current_order_id, 1)[0]
    return f"{thread} C {number}\n"


def generate_buy():
    thread, count, exact_price, local_id, symbol = generate_numbers()
    return f"{thread} B {local_id} {symbol} {exact_price} {count}\n"


def generate_numbers():
    global counter
    local_id = counter
    counter += 1
    index = random.randint(0, len(symbols) - 1)
    symbol = symbols[index]
    (low, high) = prices[index]
    exact_price = random.randint(low, high)
    count = random.randint(min_count, max_count)
    thread = random.randint(0, n_threads - 1)
    current_order_id.append((thread, local_id))
    return thread, count, exact_price, local_id, symbol


def generate_sell():
    thread, count, exact_price, local_id, symbol = generate_numbers()
    return f"{thread} S {local_id} {symbol} {exact_price} {count}\n"

def generate_symbol():
    for i in range(n_symbols):
        new_sym = ''.join(random.choices(string.ascii_uppercase, k=4))
        price_median = random.randint(min_price, max_price)
        price_range = (price_median - std_dev, price_median + std_dev)

        prices.append(price_range)
        symbols.append(new_sym)


def main():
    generate_symbol()
    fns = [generate_sell, generate_buy, generate_cancel]
    result_counter = 0
    with open(filename, 'w') as f:
        f.write(f"{n_threads}\n")
        f.write(f"o\n")
        while result_counter < n_lines:
            idx = random.randint(0, len(fns) - 1)
            fn = fns[idx]
            result = fn()
            if result:
                f.write(result)
                result_counter += 1

        f.write(f"x")


if __name__ == '__main__':
    main()
