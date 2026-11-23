import argparse
import hashlib
import json
from pathlib import Path
import statistics


def read_run(directory):
    directory = Path(directory)
    def read(name):
        return json.loads((directory/name).read_text())
    plan = read('fixed-workload.json')
    verification = read('verification.json')
    throughput = read('throughput.json')
    environment = read('environment.json')
    validity_file = directory/'performance-validity.json'
    reviewed_validity = read('performance-validity.json') if validity_file.exists() else {}
    if verification.get('performance_valid') is False or reviewed_validity.get('performance_valid') is False:
        raise ValueError('Invalid benchmark clock or sleep guard: '+str(directory))
    digest = hashlib.sha256(json.dumps(plan['baskets'], separators=(',', ':')).encode()).hexdigest()
    if digest != plan['plan_sha256']:
        raise ValueError('Plan hash mismatch: '+str(directory))
    if not all(verification.get(key) is True for key in ('passed', 'load_passed', 'notification_passed')):
        raise ValueError('Failed correctness run: '+str(directory))
    if throughput['totalTransactions'] != plan['sales'] or verification['completed_sales'] != plan['sales']+2:
        raise ValueError('Fixed sales mismatch: '+str(directory))
    count = verification['fixed_expected_notifications']
    if any(verification[key] != count for key in ('received_notifications', 'expected_notifications')):
        raise ValueError('Fixed notification mismatch: '+str(directory))
    if any(operation['errorCount'] for operation in throughput['operations']):
        raise ValueError('Workload errors: '+str(directory))
    return {'run': str(directory), 'plan': {key: value for key, value in plan.items() if key != 'baskets'},
            'notifications': count, 'tps': throughput['transactionsPerSecond'],
            'p50_ms': verification['latency_ms']['p50'], 'p95_ms': verification['latency_ms']['p95'],
            'elapsed_seconds': verification['elapsed_load_and_drain_seconds'],
            'settings': environment['notificationSettings'],
            'environment': {**{key: environment[key] for key in (
                'variant', 'catalogSize', 'initialStockPerSku', 'databaseServers', 'sharedBuffersPerInstance',
                'fsync', 'synchronous_commit', 'jvmOptions', 'subscribers', 'diagnosticsEnabled',
                'subscriptionPolicy', 'clientSourceHashes')},
                'walCompressionPerInstance': environment.get('walCompressionPerInstance', 'unrecorded'),
                'fullPageWrites': environment.get('fullPageWrites', 'unrecorded'),
                'powerConditions': verification.get('power_conditions', reviewed_validity.get('power_conditions', 'unrecorded'))}}


def compare(baselines, candidates):
    groups = [[read_run(path) for path in paths] for paths in (baselines, candidates)]
    if not all(groups):
        raise ValueError('Need both baseline and candidate runs')
    reference = groups[0][0]
    for group in groups:
        for run in group:
            if any(run[key] != reference[key] for key in ('plan', 'notifications', 'environment')):
                raise ValueError('Non-comparable workload or environment: '+run['run'])
    def summarize(group):
        return {key: statistics.median(run[key] for run in group)
                for key in ('tps', 'p50_ms', 'p95_ms', 'elapsed_seconds')}
    before, after = [summarize(group) for group in groups]
    checks = {'same_workload_and_notification_count': True,
              'all_correctness_checks_passed': True,
              'median_tps_not_lower': after['tps'] >= before['tps'],
              'median_p95_lower': after['p95_ms'] < before['p95_ms']}
    return {'passed': all(checks.values()), 'checks': checks, 'notifications_per_run': reference['notifications'],
            'baseline_medians': before, 'candidate_medians': after,
            'baseline_runs': groups[0], 'candidate_runs': groups[1],
            'note': 'Small local sequential experiment, not statistical proof. Fixed-count driver, not the original load-client. Recovery probes included equally. Source hashes remain in each environment.json.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', nargs='+', required=True)
    parser.add_argument('--candidate', nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.baseline, args.candidate)
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
