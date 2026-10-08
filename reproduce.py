"""Portable-path driver adapted from the recorded runner; not itself benchmark-validated.

See README.md for build and artifact-snapshot order. All outputs are exclusive-created.
"""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('snapshot-baseline', 'snapshot-candidate', 'rounds'))
    parser.add_argument('--spark', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    spark = args.spark.resolve()
    output = args.output.resolve()
    artifacts = output / 'artifacts'
    artifacts.mkdir(parents=True, exist_ok=True)
    catalyst = spark / 'sql/catalyst/target/scala-2.13/spark-catalyst_2.13-5.0.0-SNAPSHOT.jar'
    production = spark / ('sql/catalyst/src/main/scala/org/apache/spark/sql/'
                          'catalyst/expressions/complexTypeExtractors.scala')
    if args.action.startswith('snapshot-'):
        variant = args.action.removeprefix('snapshot-')
        destination = artifacts / f'{variant}-catalyst.jar'
        assert not destination.exists(), 'Refusing to overwrite artifact'
        provenance = json.loads((ROOT / 'provenance.json').read_text())
        assert digest(production) == provenance['artifacts'][variant]['source_sha256']
        shutil.copy2(catalyst, destination)
        with destination.with_suffix('.json').open('x') as stream:
            json.dump({'sha256': digest(destination), 'source_sha256': digest(production)}, stream)
        return
    java = Path(os.environ['JAVA_HOME']).resolve() / 'bin/java'
    options = json.loads((ROOT / 'java-options.json').read_text())
    for number in (1, 2, 3):
        for module, main_class, extra in (
            ('mllib', 'org.apache.spark.ml.feature.TargetEncoderMapLookupBenchmark', ['comparison']),
            ('sql', 'org.apache.spark.sql.execution.benchmark.RangeMapLookupControls', [])):
            for variant in (('candidate', 'baseline') if number == 2 else ('baseline', 'candidate')):
                label = f'{module}-{variant}-round-{number}'
                folder = output / label
                folder.mkdir()
                chosen = artifacts / f'{variant}-catalyst.jar'
                metadata = json.loads(chosen.with_suffix('.json').read_text())
                assert digest(chosen) == metadata['sha256']
                module_path = 'sql/core' if module == 'sql' else 'mllib'
                export = spark / module_path / 'target/streams/test/fullClasspath/_global/streams/export'
                cp = export.read_text().strip().split(':')
                assert cp.count(str(catalyst)) == 1
                assert not any(p.endswith('/sql/catalyst/target/scala-2.13/classes') for p in cp)
                cp = [str(chosen) if p == str(catalyst) else p for p in cp]
                temporary = folder / 'tmp'
                temporary.mkdir()
                flags = options + ['-Djava.io.tmpdir=' + str(temporary),
                    '-XX:ErrorFile=' + str(folder / 'hs_err_pid%p.log'),
                    '-Dspark.test.home=' + str(spark),
                    '-Dmap.lookup.expected.catalyst=' + str(chosen)]
                benchmark_args = [str(folder)] + ([variant] if module == 'sql' else extra)
                if number == 2:
                    benchmark_args.append('reverse')
                command = [str(java), *flags, '-cp', os.pathsep.join(cp), main_class, *benchmark_args]
                record = {'started': now(), 'command': command,
                          'catalyst_sha256': digest(chosen)}
                print('START', label, flush=True)
                env = dict(os.environ, SPARK_LOCAL_IP='127.0.0.1')
                env.pop('SPARK_GENERATE_BENCHMARK_FILES', None)
                with (output / (label + '.log')).open('x') as log:
                    result = subprocess.run(command, cwd=folder, env=env,
                                            stdout=log, stderr=subprocess.STDOUT)
                record.update(finished=now(), exit_code=result.returncode)
                with (output / (label + '-command.json')).open('x') as stream:
                    json.dump(record, stream, indent=2)
                assert result.returncode == 0, label
                origin = (folder / 'loaded-catalyst.txt').read_text().strip()
                assert origin in (chosen.as_uri(), 'file:' + str(chosen))
                with (folder / 'samples.csv').open() as stream:
                    samples = list(csv.reader(stream))
                assert len(samples) == (55 if module == 'sql' else 20)
                for case in {row[0] for row in samples}:
                    assert [int(row[1]) for row in samples if row[0] == case] == list(range(5))
                code_files = list(folder.glob('*-code.java'))
                assert code_files
                for path in code_files:
                    code = path.read_text()
                    assert 'mapLookupBuckets' in code
                    assert ('Murmur3_x86_32' in code) == (
                        variant == 'candidate' and not path.name.startswith('integer-'))
                print('END', label, flush=True)


if __name__ == '__main__':
    main()
