# import requests

# url = "http://104.153.122.230:8127/send-email"

# payload = {
#     "email": "krushnarajjadhav015@gmail.com",
#     "subject": "Test",
#     "body": "Hello from FastAPI!"
# }

# # Set headers manually
# headers = {"Content-Type": "application/x-www-form-urlencoded"}

# response = requests.post(url, data=payload, headers=headers)

# print(response.status_code)
# print(response.text)



import requests

def send_email_via_fastapi(email, subject, body_html):
    url = "http://104.153.122.230:8127/send-email"
    payload = {
        "email": email,
        "subject": subject,
        "body": body_html
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url, data=payload, headers=headers)

    print(f"Email sent to {email}: {response.status_code}")
    print(response.text)



def build_reins_ssns_body(ssn_list):
    term_table = ""
    if ssn_list:
        term_table = "<br/><br/><strong>Reinitiated SSNs:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th></tr>"
        for entry in ssn_list:
            term_table += f"<tr><td>{entry}</td></tr>"
        term_table += "</table>"

    body = f"""
    <p>Reinstated SSNs with date less than term date:</p>
    {term_table}
    """
    return body

def build_success_email_count_reinstate_body(term_ssn_list):
    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += f"<tr><td>{entry['SSN']}</td><td>{entry['TERM DATE']}</td></tr>"
        term_table += "</table>"

    body = f"""
    <p>Count list for reinstate SSNs:</p>
    {term_table}
    """
    return body


def build_success_email_count_body(count_list, term_ssn_list):
    count_table = """
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Source</th><th>Total Subscribers</th><th>Total Spouse</th><th>Other Dependents</th></tr>
        <tr><td>Excel</td><td>{}</td><td>{}</td><td>{}</td></tr>
        <tr><td>DB2</td><td>{}</td><td>{}</td><td>{}</td></tr>
    </table>
    """.format(
        count_list[0]['excel'][0]['total_subscriber'], 
        count_list[0]['excel'][1]['total_spouse'], 
        count_list[0]['excel'][2]['other dependents'],
        count_list[1]['db2'][0]['total_subscriber'], 
        count_list[1]['db2'][1]['total_spouse'], 
        count_list[1]['db2'][2]['other dependents']
    )

    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += f"<tr><td>{entry['SSN']}</td><td>{entry['TERM DATE']}</td></tr>"
        term_table += "</table>"

    body = f"""
    <p>Count list for both Excel and DB2:</p>
    {count_table}
    {term_table}
    """
    return body

count_list = [
    {
        'excel': [
            {'total_subscriber': 120},
            {'total_spouse': 45},
            {'other dependents': 30}
        ]
    },
    {
        'db2': [
            {'total_subscriber': 118},
            {'total_spouse': 44},
            {'other dependents': 31}
        ]
    }
]


term_ssn_list = [
    {'SSN': '123-45-6789', 'TERM DATE': '2025-06-01'},
    {'SSN': '987-65-4321', 'TERM DATE': '2025-05-20'},
    {'SSN': '555-66-7777', 'TERM DATE': '2025-04-15'}
]

# html_body = build_success_email_count_body(count_list,term_ssn_list)
# send_email_via_fastapi("krushnarajjadhav015@gmail.com","subject",html_body)

def build_success_email_term_body(count_list):
    body = f"""
    <p>These are the SSNs added to DB2 and Excel with a Term date {count_list}.</p>
    """
    return body


def build_missing_ssn_email_body(missing_ssn, missing_in_inventory):
    body = f"""
    <html>
    <body>
        <p>Hello,</p>

        <p>Here is the report for Member IDs:</p>

        <h3>Missing SSN (present in inventory, not in current data):</h3>
        <p>{', '.join(missing_ssn) if missing_ssn else 'None'}</p>

        <h3>Missing inventory SSN (present in current data, not in inventory):</h3>
        <p>{', '.join(missing_in_inventory) if missing_in_inventory else 'None'}</p>

        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_error_log_email_body(file_name, error_message, error_logs):
    # Convert error_logs to HTML table
    table_rows = ""
    for key, value in error_logs.items():
        table_rows += f"<tr><td>{key}</td><td>{value}</td></tr>"

    html_table = f"""
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Member ID</th><th>Group Number</th></tr>
        {table_rows}
    </table>
    """

    body = f"""
    <html>
    <body>
        <p><strong>Group numbers not found for the following member ID(s):</strong> {error_message}</p>
        <br>
        {html_table}
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_member_id_email_body(new_ids, missing_ids):
    body = f"""
    <html>
    <body>
        <p>Hello,</p>

        <p>Here is the report for Member IDs:</p>

        <h3>New Member IDs (present today, not in previous data):</h3>
        <p>{', '.join(new_ids) if new_ids else 'None'}</p>

        <h3>Missing Member IDs (present in previous data, not today):</h3>
        <p>{', '.join(missing_ids) if missing_ids else 'None'}</p>

        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_success_email_body(file_name):
    body = f"""
    <html>
    <body>
        <p>The file <strong>{file_name}</strong> was processed successfully.</p>
        <p>The processed file and original input are available on the server.</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_error_email_body(file_name, error_message):
    body = f"""
    <html>
    <body>
        <p>The file <strong>{file_name}</strong> failed to process.</p>
        <p><strong>Reason:</strong> {error_message}</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body



def build_error_email_variance_body(file_name, p_type):
    body = f"""
    <html>
    <body>
        <p>The file <strong>{file_name}</strong> has a variance in <strong>{p_type}</strong> of more than five percent.</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_success_email_count_single_body(count_list, term_ssn_list):
    count_table = """
    <table border="1" cellpadding="5" cellspacing="0">
        <tr><th>Source</th><th>Total Subscribers</th><th>Total Spouse</th><th>Other Dependents</th></tr>
        <tr><td>Excel</td><td>{}</td><td>{}</td><td>{}</td></tr>
        <tr><td>DB2</td><td>{}</td><td>{}</td><td>{}</td></tr>
    </table>
    """.format(
        count_list[0]['excel'][0]['total_subscriber'], 
        count_list[0]['excel'][1]['total_spouse'], 
        count_list[0]['excel'][2]['other dependents'],
        count_list[1]['db2'][0]['total_subscriber'], 
        count_list[1]['db2'][1]['total_spouse'], 
        count_list[1]['db2'][2]['other dependents']
    )

    term_table = ""
    if term_ssn_list:
        term_table = "<br/><br/><strong>Term SSN Added to DB2 from Inventory:</strong><table border='1' cellpadding='5' cellspacing='0'><tr><th>SSN</th><th>TERM DATE</th></tr>"
        for entry in term_ssn_list:
            term_table += "<tr><td>{}</td><td>{}</td></tr>".format(entry['SSN'], entry['TERM DATE'])
        term_table += "</table>"

    body = f"""
    <html>
    <body>
        <p>Count list for both Excel and DB2:</p>
        {count_table}
        {term_table}
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_success_email_term_body(count_list):
    body = f"""
    <html>
    <body>
        <p>These are the SSN(s) added to DB2 and Excel with a Term date:</p>
        <p>{count_list}</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body


def build_success_email_count_everything_fine_body(count_list):
    body = f"""
    <html>
    <body>
        <p>Count matched for both Excel and DB2:</p>
        <p>{count_list}</p>
        <br>
        <p>Best regards,</p>
        <p>Your System</p>
    </body>
    </html>
    """
    return body
