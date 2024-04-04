import random
from typing import Callable
from datetime import datetime, timedelta
from time import sleep
import shutil, os, tempfile, csv
from contextlib import contextmanager

PRODUCTS = ['Brush', 'Socks', 'Lamp', 'Spice', 'Juice', 'Cable', 'Rice', 'Soap', 'Nut', 'Bolt', 'Jeans', 'Lens', 'Bag', 'Yarn', 'Mug', 'Pen', 'Key', 'Seed', 'Comb', 'Glue', 'Wax', 'Mask', 'Chip', 'Herb', 'Bean', 'Tape', 'Jar', 'Nail', 'Fork', 'Tea' ]
MAX_QTY = 10
MIN_PRICE = 0.50
MAX_PRICE = 100.00

def sleep_until(target: datetime):
    sleep((target - datetime.now()).total_seconds())
    return

def prepare_dir(dir):
    shutil.rmtree(dir, ignore_errors=True)
    os.mkdir(dir)
    return

@contextmanager
def step_file(dir, step_i):
    tmp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    writer = csv.writer(tmp_file)
    try:
        yield writer
    finally:
        tmp_file_name = tmp_file.name
        tmp_file.close()
        shutil.copy(tmp_file_name, f'{dir}/{step_i:>05}.csv')
        os.remove(tmp_file_name)
    return

def gen_sales_batch(num_products, timestamp: datetime, delay_prob, max_delay):
    batch = list()
    for product in PRODUCTS[0:num_products]:
        qty = random.randint(1, MAX_QTY)
        emit_timestamp = timestamp
        if random.random() <= delay_prob:
            emit_timestamp += timedelta(0, random.randint(1, max_delay))
        batch.append((emit_timestamp, (timestamp.isoformat(), product, qty)))
    return batch

def gen_prices_batch(num_products, timestamp: datetime, delay_prob, max_delay):
    batch = list()
    for product in PRODUCTS[0:num_products]:
        price = round((random.random() * (MAX_PRICE - MIN_PRICE) + MIN_PRICE) * 100) / 100
        emit_timestamp = timestamp
        if random.random() <= delay_prob:
            emit_timestamp += timedelta(0, random.randint(1, max_delay))
        batch.append((emit_timestamp, (timestamp.isoformat(), product, price)))
    return batch

SourceSpec = tuple[str, int, Callable, float, int]
""" directory, frequency (one batch every # of steps), generation function,
delay probability, max delay (seconds).
"""

def gen_streams(start_timestamp: datetime, step_size: int, num_steps: int,
                num_products: int,
                sources: list[SourceSpec]):
    for dir, *_ in sources:
        prepare_dir(dir)
    random.seed(0)
    timestamp = start_timestamp
    delayed_queues: list[list[tuple[datetime, tuple]]] = [list() for _ in sources]
    step_i = 0
    while step_i < num_steps or any(len(q) > 0 for q in delayed_queues):
        wall_clock = datetime.now()
        for (dir, freq, gen_source_batch, delay_prob, max_delay), delayed_queue in zip(sources, delayed_queues):
            with step_file(dir, step_i) as f:
                for emit_timestamp, row in delayed_queue:
                    if emit_timestamp <= timestamp:
                        f.writerow(row)
                delayed_queue[:] = [ (emit_timestamp, row) for emit_timestamp, row in delayed_queue if emit_timestamp > timestamp ]
                if step_i < num_steps and step_i % freq == 0:
                    for emit_timestamp, row in gen_source_batch(num_products, timestamp, delay_prob, max_delay):
                        if emit_timestamp <= timestamp:
                            f.writerow(row)
                        else:
                            delayed_queue.append((emit_timestamp, row))
        step_i += 1
        timestamp += timedelta(0, step_size)
        sleep_until(wall_clock + timedelta(0, step_size))
    return
