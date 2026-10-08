"""Recompute every published timing from included raw samples; no exclusions."""
import csv
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent


def analyze():
    total = 0
    outliers = 0
    for family, expected in (('target-encoder', 20), ('range', 55)):
        medians = {}
        case_sets = []
        for variant in ('baseline', 'candidate'):
            for number in (1, 2, 3):
                with (ROOT / 'raw' / family / f'{variant}-round-{number}.csv').open() as stream:
                    rows = list(csv.reader(stream))
                assert len(rows) == expected
                total += len(rows)
                cases = sorted({row[0] for row in rows})
                case_sets.append(cases)
                for case in cases:
                    samples = [row for row in rows if row[0] == case]
                    assert [int(row[1]) for row in samples] == list(range(5))
                    values = [int(row[2]) / 1e6 for row in samples]
                    assert min(values) > 0
                    median = statistics.median(values)
                    medians[(variant, number, case)] = median
                    outliers += sum(value > 2 * median for value in values)
        assert all(cases == case_sets[0] for cases in case_sets)
        print(f'\n### {family}\n')
        print('| Case | Baseline ms | Candidate ms | Paired speedup | Paired runtime change |')
        print('|---|---:|---:|---:|---:|')
        for case in case_sets[0]:
            baseline = [medians[('baseline', n, case)] for n in (1, 2, 3)]
            candidate = [medians[('candidate', n, case)] for n in (1, 2, 3)]
            ratios = [b / c for b, c in zip(baseline, candidate)]
            changes = [100 * (c / b - 1) for b, c in zip(baseline, candidate)]
            print(f'| {case} | {statistics.median(baseline):.2f} | '
                  f'{statistics.median(candidate):.2f} | {min(ratios):.2f}-{max(ratios):.2f}x | '
                  f'{min(changes):+.1f}% to {max(changes):+.1f}% |')
    assert total == 450
    print(f'\nAll {total} samples retained; {outliers} exceed twice their five-sample group median.')


if __name__ == '__main__':
    analyze()
