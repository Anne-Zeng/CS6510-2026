def prepare_inventory(database, opening_stock):
    if type(opening_stock) is not int or not 1 <= opening_stock <= 2_000_000_000:
        raise ValueError('opening-stock must be an integer from 1 through 2000000000')
    database.sql(f'UPDATE inventory SET current_stock={opening_stock}')
    return {'opening_stock_per_sku': opening_stock,
            'sku_count': int(database.sql('SELECT count(*) FROM inventory'))}


def assess_workload(report):
    expected = {'START_TRANSACTION', 'SCAN_ITEM', 'COMPLETE_TRANSACTION'}
    operations = report.get('operations', [])
    names = [entry.get('operation') for entry in operations]
    complete = len(names) == len(expected) and set(names) == expected
    counts_valid = complete and all(
        type(entry.get(field)) is int and entry[field] >= 0
        for entry in operations for field in ('successCount', 'errorCount'))
    errors = {entry['operation']: entry['errorCount'] for entry in operations} if counts_valid else {}
    transactions = report.get('totalTransactions')
    completed = next((entry.get('successCount') for entry in operations
                      if entry.get('operation') == 'COMPLETE_TRANSACTION'), None)
    positive = type(transactions) is int and transactions > 0
    passed = counts_valid and positive and transactions == completed and sum(errors.values()) == 0
    return {'passed': passed, 'criterion': 'positive_completed_transactions_and_zero_operation_errors',
            'report_valid': counts_valid and positive and transactions == completed,
            'operation_errors': errors, 'total_operation_errors': sum(errors.values()) if counts_valid else None}
