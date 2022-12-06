import json

import time
import argparse
import shutil

from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Tool to merge logs.')

    parser.add_argument(
        'input_dir_log_a',
        metavar='<INPUT DIR_A>',
        type=str,
        help='path to dir with generated logs',
    )

    parser.add_argument(
        'input_dir_log_b',
        metavar='<INPUT DIR_B>',
        type=str,
        help='path to dir with generated logs',
    )

    parser.add_argument(
        '-o',
        '--open',
        action='store_const',
        const=True,
        default=False,
    )

    parser.add_argument(
        'output_dir_logs',
        metavar='<OUTPUT DIR>',
        type=str,
        help='path to dir with generated logs',
    )

    parser.add_argument(
        '-f', '--force',
        action='store_const',
        const=True,
        default=False,
        help='force write logs',
        dest='force_write',
    )

    return parser.parse_args()


def _create_dir(dir_path: Path, *, force_write: bool = False) -> None:
    if dir_path.exists():
        if not force_write:
            raise FileExistsError(
                f'Dir "{dir_path}" already exists. Remove it first or choose another one.')
        shutil.rmtree(dir_path)

    dir_path.mkdir(parents=True)


def _merge_logs(dir_path_a: Path, dir_path_b: Path, output_dir_logs: Path):
    log_path = output_dir_logs.joinpath('merge_logs.jsonl')
    print(f'merging and sorting logs to {log_path.name}...')

    log_a = _read_logs(dir_path_a, 'fha')
    log_b = _read_logs(dir_path_b, 'fhb')
    a = next(log_a)
    b = next(log_b)

    with log_path.open('w') as fh:
        while True:
            if a['timestamp'] <= b['timestamp']:
                fh.write(json.dumps(a) + "\n")
                try:
                    a = next(log_a)
                except StopIteration:
                    fh.write(json.dumps(b) + "\n")
                    for lb in log_b:
                        fh.write(json.dumps(lb) + "\n")
                    break
            else:
                fh.write(json.dumps(b) + "\n")
                try:
                    b = next(log_b)
                except StopIteration:
                    fh.write(json.dumps(a) + "\n")
                    for la in log_a:
                        fh.write(json.dumps(la) + "\n")
                    break


def _read_logs(dir_path: Path, file_name) -> dict:
    with dir_path.open('r', encoding='utf-8') as file_name:
        for line in file_name:
            r = json.loads(line)
            yield r


def main() -> None:
    args = _parse_args()

    t0 = time.time()
    input_dir_log_a = Path(args.input_dir_log_a)
    input_dir_log_b = Path(args.input_dir_log_b)
    output_dir_logs = Path(args.output_dir_logs)
    _create_dir(output_dir_logs, force_write=args.force_write)
    _merge_logs(input_dir_log_a, input_dir_log_b, output_dir_logs)
    #print(memory_usage((_merge_logs, (input_dir_log_a, input_dir_log_b, output_dir_logs), {})))
    print(f"finished in {time.time() - t0:0f} sec")


if __name__ == '__main__':
    main()