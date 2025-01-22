import json
import re
import pendulum

# 전역 변수로 real_time 설정
real_time = pendulum.now("Asia/Seoul")

class DataUtil:
  format_mapping = {
    'YYYY': 'YYYY',
    'MM': 'MM',
    'DD': 'DD',
    'YYYYMM': 'YYYYMM',
    'YYYYMMDD': 'YYYYMMDD',
    'YYYYMMDDHH': 'YYYYMMDDHH',
    'YYYYMMDDHHMI': 'YYYYMMDDHHmm',
    'YYYYMMDDHHMISS': 'YYYYMMDDHHmmss',
    'YYYYMMDDHHMISSSSS': 'YYYYMMDDHHmmssSSS',
    'EOM': 'YYYYMMDD'
  }

  @classmethod
  def process_timestamp(cls, match):
    global real_time
    format_str = match.group(1)
    params = match.group(2).split(',') if match.group(2) else []

    if format_str not in cls.format_mapping:
      return match.group(0)  # Return original if format not recognized

    # EOM 처리 (기존 if len(params) == 3: 전에 추가)
    if format_str == 'EOM':
      base_time = real_time.end_of('month')

      if len(params) == 3:
        unit = params[1].strip().upper()
        try:
          value = int(params[2])
          if unit == 'DD':
            adjusted_time = base_time.add(days=value)
          elif unit == 'MM':
            adjusted_time = base_time.add(months=value).end_of('month')
          elif unit == 'YYYY':
            adjusted_time = base_time.add(years=value).end_of('month')
          else:
            return base_time.format(cls.format_mapping[format_str])
          return adjusted_time.format(cls.format_mapping[format_str])
        except ValueError:
          return base_time.format(cls.format_mapping[format_str])
      return base_time.format(cls.format_mapping[format_str])

    if len(params) == 0:
      # 단순 형식 (예: ${YYYYMM})
      return real_time.format(cls.format_mapping[format_str])
    elif len(params) == 3:
      # 계산이 필요한 형식 (예: ${YYYYMM, YYYY, +1})
      unit = params[1].strip().upper()
      try:
        value = int(params[2])
      except ValueError:
        return real_time.format(cls.format_mapping[format_str])  # 숫자가 아닌 경우 원래 시간 반환

      # 단위에 따라 계산 수행
      if unit == 'YYYY':
        adjusted_time = real_time.add(years=value)
      elif unit == 'MM':
        adjusted_time = real_time.add(months=value)
      elif unit == 'DD':
        adjusted_time = real_time.add(days=value)
      elif unit == 'HH':
        adjusted_time = real_time.add(hours=value)
      elif unit == 'MI':
        adjusted_time = real_time.add(minutes=value)
      elif unit == 'SS':
        adjusted_time = real_time.add(seconds=value)
      else:
        return real_time.format(cls.format_mapping[format_str])  # 알 수 없는 단위일 경우 원래 시간 반환

      # 계산된 시간을 원래 형식에 맞춰 포맷팅
      return adjusted_time.format(cls.format_mapping[format_str])
    else:
      return real_time.format(cls.format_mapping[format_str])

  @classmethod
  def replace_timestamps(cls, obj):
    if isinstance(obj, dict):
      return {k: cls.replace_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, list):
      return [cls.replace_timestamps(item) for item in obj]
    elif isinstance(obj, str):
      return re.sub(r'\$\{(\w+)(.*?)\}', cls.process_timestamp, obj)
    else:
      return obj

  @classmethod
  def process_template(cls, data):
    if data is None:
      return None

    if isinstance(data, str):
      try:
        # Try parsing as JSON first
        parsed_data = json.loads(data)
        processed_data = cls.replace_timestamps(parsed_data)
        return json.dumps(processed_data, indent=2)
      except json.JSONDecodeError:
        # If not JSON, treat as a simple string
        return cls.replace_timestamps(data)
    elif isinstance(data, (dict, list)):
      # If already a Python object (dict or list), process it directly
      processed_data = cls.replace_timestamps(data)
      return processed_data  # 리스트나 딕셔너리인 경우 JSON으로 변환하지 않고 그대로 반환
    else:
      # 다른 타입의 데이터는 그대로 반환
      return data