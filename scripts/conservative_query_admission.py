"""Read-only provenance checks for the reviewed development-only exception.

This validates recorded evidence and reviewer decisions. It does not infer whether
two observations mean the same thing or turn a failed diagnostic into a pass.
"""
import hashlib
import json
import math
from pathlib import Path, PurePosixPath


def sha(data):
    return hashlib.sha256(data).hexdigest()


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate admission key')
            result[key] = value
        return result
    def invalid(_value):
        raise ValueError('Nonfinite admission value')
    return json.loads(data, object_pairs_hook=pairs, parse_constant=invalid)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def counts(record, expected):
    require(all(type(record.get(key)) is int and record[key] == value
                for key, value in expected.items()), 'Missing or ineligible admission counts')


def reviewers(record):
    values = record.get('reviewers')
    require(isinstance(values, list) and len(values) == 2
            and all(isinstance(v, str) and v.strip() for v in values)
            and len({v.strip() for v in values}) == 2, 'Independent reviewers required')


def _transport(model, expected):
    require(model.get('status') == 'completed', 'Incomplete probe transport')
    chunks = model.get('chunks')
    require(isinstance(chunks, list) and chunks, 'Missing probe transport chunks')
    text, finished = [], False
    for chunk in chunks:
        for choice in chunk.get('choices', []):
            require(type(choice.get('index')) is int and choice['index'] == 0,
                    'Unexpected probe choice')
            delta = choice.get('delta', {})
            require(not any(delta.get(k) for k in ('refusal', 'function_call', 'tool_calls')),
                    'Probe refusal or tool call')
            if delta.get('content'):
                require(not finished, 'Probe content after termination')
                text.append(delta['content'])
            reason = choice.get('finish_reason')
            if reason is not None:
                require(reason == 'stop' and not finished, 'Probe abnormal termination')
                finished = True
    require(finished and strict_json(''.join(text)) == expected, 'Probe terminal response mismatch')


def _probe(snapshot, prefix, known, input_sha):
    def raw(name):
        return snapshot[f'{prefix}/{name}']
    def read(name):
        return strict_json(raw(name))
    manifest = read('manifest.json')
    result_bytes = raw('results/result.json')
    result, review = strict_json(result_bytes), read('results/review.json')
    require(manifest.get('experiment') == 'exclusion-authority'
            and manifest.get('input_sha256') == input_sha == sha(raw('inputs.json'))
            and result.get('input_sha256') == input_sha
            and result.get('manifest_sha256') == sha(raw('manifest.json')),
            'Probe manifest/input binding mismatch')
    counts(manifest, {'max_calls': 9, 'active_seconds': 900, 'sdk_retries': 0})
    require(manifest.get('output_token_cap') is None and manifest.get('strands_retries') is None
            and manifest.get('proxy_cache') == 'bypass', 'Probe execution policy mismatch')
    for name, expected_sha in manifest['reviews'].items():
        receipt = read(name)
        require(sha(raw(name)) == expected_sha and receipt.get('verdict') == 'pass'
                and receipt.get('input_sha256') == input_sha, 'Probe input review mismatch')
    counts(result, {'native_call_count': 9, 'not_started_calls': 0})
    require(result.get('complete_execution') is True and result.get('interrupted') is None
            and result.get('close_error') is None
            and type(result.get('elapsed_seconds')) in (int, float)
            and math.isfinite(result['elapsed_seconds']) and 0 <= result['elapsed_seconds'] <= 900,
            'Incomplete probe execution')
    reviewers(review)
    eligible = {'false_exclusion_approvals': 0, 'false_exclusion_rejections': 1,
                'wrong_targets': 0, 'transport_failures': 0}
    counts(review, eligible)
    require(review.get('result_sha256') == sha(result_bytes)
            and review.get('complete_execution') is True, 'Probe aggregate binding mismatch')
    for axis in ('spec', 'standards'):
        grade_bytes = raw(f'results/grade-{axis}.json')
        grade = strict_json(grade_bytes)
        require(review.get(axis) == 'fail' and grade.get('verdict') == 'fail'
                and grade.get('result_sha256') == sha(result_bytes)
                and review.get('grade_sha256', {}).get(axis) == sha(grade_bytes),
                'Failed probe grade binding mismatch')
        counts(grade, eligible)
    for field, stem, digits in (('capture_sha256', 'model', 3),
                                ('stage_sha256', 'stage', 3), ('call_sha256', 'call', 2)):
        names = ({f'{stem}-{i:0{digits}d}-{kind}.json' for i in range(9)
                  for kind in ('input', 'output')} if stem != 'call'
                 else {f'call-{i:02d}.json' for i in range(9)})
        require(result.get(field) == {n: sha(raw(f'results/{n}')) for n in sorted(names)},
                'Probe capture hash mismatch')
    calls = read('inputs.json')['calls']
    require(len(calls) == 9 and len({c['id'] for c in calls}) == 9
            and len(result.get('calls', [])) == 9, 'Probe schedule mismatch')
    errors = []
    for index, call in enumerate(calls):
        row = read(f'results/call-{index:02d}.json')
        require(row == result['calls'][index] and type(row.get('index')) is int
                and row['index'] == index and row.get('id') == call['id']
                and row.get('status') == 'valid_ungraded', 'Probe call identity mismatch')
        response = row['response']
        stage_in, stage = (read(f'results/stage-{index:03d}-{kind}.json')
                           for kind in ('input', 'output'))
        model_in, model = (read(f'results/model-{index:03d}-{kind}.json')
                           for kind in ('input', 'output'))
        require(all(type(item.get('index')) is int and item['index'] == index
                    for item in (stage_in, stage, model_in, model)), 'Probe artifact identity mismatch')
        require(stage.get('outcome') == 'completed'
                and stage.get('native_result', {}).get('stop_reason') == 'end_turn'
                and strict_json(stage['response']) == response
                and strict_json(stage['native_result']['text']) == response,
                'Probe native response mismatch')
        for item in (stage_in, stage):
            require(item.get('name') == 'fact_exclusion'
                    and strict_json(item['prompt']) == call['payload']
                    and sha(item['system_prompt'].encode()) == manifest['prompt_sha256'],
                    'Probe original input mismatch')
        request = model_in['request']
        require(request == model['request'] and request.get('model') == manifest['runtime']['model']
                and request.get('messages') == [
                    {'role': 'system', 'content': stage_in['system_prompt']},
                    {'role': 'user', 'content': [{'text': stage_in['prompt'], 'type': 'text'}]}],
                'Probe requested model/input mismatch')
        _transport(model, response)
        decisions = response.get('decisions')
        require(set(response) == {'decisions'} and isinstance(decisions, list) and len(decisions) == 1,
                'Probe classification shape mismatch')
        decision, gold = decisions[0], call['gold']
        require(set(decision) == {'observation_id', 'decision', 'target_id'}
                and decision['observation_id'] == call['payload']['omitted_id'],
                'Probe classification identity mismatch')
        correct = decision['decision'] == gold['decision'] and (
            decision['target_id'] in gold['target_ids'] if gold['decision'] == 'covered_by'
            else decision['target_id'] is None)
        if not correct:
            require(call['id'] == known['case_id'] and decision['observation_id'] == known['omitted_id']
                    and decision['decision'] == 'reject' and decision['target_id'] is None
                    and gold['decision'] == 'covered_by' and known['target_id'] in gold['target_ids']
                    and known['target_id'] in call['payload']['delivered_ids'],
                    'Probe contains an ineligible semantic error')
            errors.append(call['id'])
    require(errors == [known['case_id']], 'Known full-duplicate rejection required')
    return manifest


def load_admission(path, *, code_sha256, runtime, policy_bytes):
    """Return content-free provenance after validating one immutable byte snapshot."""
    path = Path(path)
    receipt_bytes = path.read_bytes()
    receipt = strict_json(receipt_bytes)
    require(set(receipt) == {'version', 'kind', 'policy_sha256', 'input_sha256',
                             'known_rejection', 'artifacts_sha256'}
            and type(receipt['version']) is int and receipt['version'] == 1
            and receipt['kind'] == 'conservative-duplicate-development', 'Invalid conservative admission')
    known = receipt['known_rejection']
    require(isinstance(known, dict) and set(known) == {'case_id', 'omitted_id', 'target_id'}
            and all(isinstance(v, str) and v.strip() for v in known.values()), 'Invalid known rejection')
    hashes = receipt['artifacts_sha256']
    require(isinstance(hashes, dict) and hashes, 'Admission evidence required')
    inventory = {str(p.relative_to(path.parent)) for prefix in ('primary', 'comparison')
                 for p in (path.parent / prefix).rglob('*') if p.is_file()}
    inventory.update(('policy.md', 'policy-spec-review.json', 'policy-standards-review.json'))
    require(set(hashes) == inventory, 'Admission evidence inventory changed')
    snapshot = {}
    for name, expected in hashes.items():
        rel = PurePosixPath(name)
        require(not rel.is_absolute() and '..' not in rel.parts and str(rel) == name
                and (rel.parts[0] in {'primary', 'comparison'} or name in {
                    'policy.md', 'policy-spec-review.json', 'policy-standards-review.json'}),
                'Invalid admission evidence path')
        file = path.parent / name
        require(not any(path.parent.joinpath(*rel.parts[:i]).is_symlink()
                        for i in range(1, len(rel.parts) + 1)), 'Admission symlinks are not allowed')
        snapshot[name] = file.read_bytes()
        require(sha(snapshot[name]) == expected, 'Admission evidence bytes changed')
    require(receipt['policy_sha256'] == sha(policy_bytes) == sha(snapshot['policy.md']),
            'Admission policy changed')
    identities = []
    for axis in ('spec', 'standards'):
        review = strict_json(snapshot[f'policy-{axis}-review.json'])
        identity = review.get('reviewer')
        require(review.get('verdict') == 'pass' and review.get('policy_sha256') == receipt['policy_sha256']
                and isinstance(identity, str) and identity.strip(), 'Independent policy approval required')
        identities.append(identity.strip())
    require(len(set(identities)) == 2, 'Independent policy reviewers required')
    primary = _probe(snapshot, 'primary', known, receipt['input_sha256'])
    comparison = _probe(snapshot, 'comparison', known, receipt['input_sha256'])
    require(primary['runtime'] == runtime and runtime['model'] == 'gemini-3.8-flash'
            and comparison['runtime']['model'] == 'gpt-5.5', 'Admission runtime changed')
    comparison['runtime']['model'] = primary['runtime']['model']
    require(comparison == primary, 'Probe candidates differ beyond reviewed model comparison')
    def application(values):
        return {k: v for k, v in values.items() if k.startswith('app/') or k == 'requirements.lock'}
    current = application(code_sha256)
    require(current and 'requirements.lock' in current and application(primary['code_sha256']) == current,
            'Admission application or lock changed')
    return {**receipt, 'receipt_sha256': sha(receipt_bytes)}


def validate_case_grades(snapshot, result, review):
    """Bind dual semantic judgments to surviving targets, without judging meaning."""
    reviewers(review)
    result_sha = sha(snapshot['result.json'])
    expected = {'raw_false_approvals': 0, 'delivered_false_approvals': 0,
                'missing_required_aspects': 0, 'false_complete_coverage': 0,
                'unsupported_extras': 0, 'false_exclusion_approvals': 0}
    counts(review, expected)
    underreported = review.get('coverage_underreported_aspects')
    require(type(underreported) is int and underreported >= 0, 'Explicit coverage under-reporting count required')
    judgments = []
    for axis in ('spec', 'standards'):
        payload = snapshot[f'grade-{axis}.json']
        grade = strict_json(payload)
        require(grade.get('verdict') == 'pass' and grade.get('result_sha256') == result_sha
                and review.get('grade_sha256', {}).get(axis) == sha(payload),
                'Conservative admission requires bound individual grades')
        counts(grade, expected)
        require(type(grade.get('coverage_underreported_aspects')) is int
                and grade['coverage_underreported_aspects'] == underreported, 'Coverage review disagreement')
        count, targets = grade.get('conservative_duplicate_rejections'), grade.get('conservative_duplicate_targets')
        require(type(count) is int and count >= 0 and isinstance(targets, list) and len(targets) == count,
                'Explicit conservative rejection counts and targets required')
        require(all(isinstance(t, dict) and set(t) == {'omitted_id', 'target_id', 'final_claim_text'}
                    and all(isinstance(v, str) and v.strip() for v in t.values()) for t in targets)
                and len({t['omitted_id'] for t in targets}) == count, 'Invalid conservative rejection targets')
        judgments.append((count, targets))
    require(judgments[0] == judgments[1], 'Conservative duplicate judgments disagree')
    count, targets = judgments[0]
    require(type(review.get('conservative_duplicate_rejections')) is int
            and review['conservative_duplicate_rejections'] == count
            and review.get('conservative_duplicate_targets') == targets, 'Aggregate conservative judgment mismatch')
    final = result['final']
    receipt = final['finalization']['fact_conservation']
    if receipt.get('version') == 4:
        require(count == 0 and not targets and not {'dispositions', 'reviews'} & set(receipt),
                'Reader inventory cannot use semantic exclusion authority')
        for axis in ('spec', 'standards'):
            counts(strict_json(snapshot[f'grade-{axis}.json']), {'fact_filter_calls': 0})
        counts(review, {'fact_filter_calls': 0})
        stages = {name: strict_json(payload) for name, payload in snapshot.items()
                  if name.startswith(('attempt-', 'stage-'))
                  and name.endswith(('-input.json', '-output.json'))}
        inputs = {name for name in stages if name.endswith('-input.json')}
        outputs = {name for name in stages if name.endswith('-output.json')}
        require(inputs and outputs == {name.replace('-input.json', '-output.json') for name in inputs}
                and all(isinstance(row.get('name'), str) and row['name']
                        and row['name'] not in {'fact_selector', 'fact_exclusion'}
                        for row in stages.values())
                and all(stages[name]['name'] == stages[name.replace('-input.json', '-output.json')]['name']
                        for name in inputs),
                'Reader inventory contains missing, mismatched or forbidden stage evidence')
        return
    mappings = {r['observation_id']: r for r in receipt['mappings']}
    dispositions = {r['observation_id']: r['status'] for r in receipt['dispositions']}
    reviews = {r['observation_id']: r for r in receipt['reviews']}
    claims = {c['id']: c for c in final['claim_ledger']['claims']}
    for target in targets:
        omitted, selected = target['omitted_id'], target['target_id']
        mapping = mappings.get(selected, {})
        claim = claims.get(mapping.get('unit_id'), {})
        require(dispositions.get(omitted) == 'omitted' and dispositions.get(selected) == 'delivered'
                and reviews.get(omitted, {}).get('decision') == 'reject'
                and mapping.get('status') == 'preserved' and claim.get('status') == 'supported'
                and claim.get('claim') == target['final_claim_text'],
                'Conservative duplicate target did not survive final delivery')
