import sys
sys.path.insert(0, r'C:\Users\Notandi\code\sh-apk-api')

filepath = r'C:\Users\Notandi\code\sh-apk-api\app\main.py'

with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the date parsing in list_records endpoint
old = '{"start_date": start_date, "end_date": end_date},'
new = '{"start_date": datetime.strptime(start_date, "%Y-%m-%d").date(), "end_date": datetime.strptime(end_date, "%Y-%m-%d").date()},'

if old in content:
    content = content.replace(old, new, 1)
    print('Fixed list_records!')
else:
    print('Pattern not found in list_records')

# Write back
with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print('Done!')
