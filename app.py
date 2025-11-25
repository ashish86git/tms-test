from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash, jsonify, make_response, g
import pandas as pd
import os
import io

from urllib.parse import quote_plus
import requests, time, math
from ortools.constraint_solver import routing_enums_pb2, pywrapcp
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from psycopg2.extras import RealDictCursor
import psycopg2
from datetime import datetime, date, timedelta

# -------------------------- Configuration and Initialization --------------------------

UPLOAD_FOLDER = os.path.join('static', 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = 'tms-secret-key'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Database configuration
db_config = {
    'host': 'c7s7ncbk19n97r.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com',
    'user': 'u7tqojjihbpn7s',
    'password': 'p1b1897f6356bab4e52b727ee100290a84e4bf71d02e064e90c2c705bfd26f4a5',
    'database': 'd8lp4hr6fmvb9m',
    'port': 5432
}


def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        dbname=db_config['database'],
        port=db_config['port']
    )
    return conn

# Database table creation function
def create_tables():
    """Creates the necessary tables if they do not already exist."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Create driver_master table (if it doesn't exist)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS driver_master (
                driver_id VARCHAR(50) PRIMARY KEY,
                driver_name VARCHAR(100) NOT NULL,
                license_number VARCHAR(50) NOT NULL,
                contact_number VARCHAR(20),
                address TEXT,
                availability VARCHAR(20),
                shift_info VARCHAR(50),
                aadhar_file VARCHAR(255),
                license_file VARCHAR(255)
            );
        """)

        # Create driver_financials table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS driver_financials (
                financial_id SERIAL PRIMARY KEY,
                driver_id VARCHAR(50) REFERENCES driver_master(driver_id) ON DELETE CASCADE,
                salary NUMERIC(10, 2) NOT NULL,
                bonus NUMERIC(10, 2) DEFAULT 0.00,
                last_paid_date DATE
            );
        """)

        conn.commit()
        print("Tables created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
    finally:
        if conn:
            conn.close()

# Create tables when the application starts
with app.app_context():
    create_tables()


# -------------------------- Authentication Routes --------------------------

@app.route('/', methods=['GET', 'POST'])
def auth():
    """Handles user login and signup."""
    if request.method == 'POST':
        form_type = request.form.get('form_type')

        if form_type == 'login':
            username = request.form['username']
            password = request.form['password']

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM users_tms WHERE username = %s', (username,))
            user = cursor.fetchone()
            cursor.close()
            conn.close()

            if user and (user['password'] == password or check_password_hash(user['password'], password)):
                session['user'] = username
                return redirect(url_for('dashboard'))

            return render_template('login.html', error='Invalid username or password', form_type='login')

        elif form_type == 'signup':
            username = request.form['username']
            email = request.form['email']
            password = request.form['password']

            conn = get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute('SELECT * FROM users_tms WHERE username = %s', (username,))
            existing_user = cursor.fetchone()

            if existing_user:
                cursor.close()
                conn.close()
                return render_template('login.html', error='Username already exists', form_type='signup')
            else:
                hashed_password = generate_password_hash(password)
                cursor.execute(
                    'INSERT INTO users_tms (username, email, password) VALUES (%s, %s, %s)',
                    (username, email, hashed_password)
                )
                conn.commit()
                cursor.close()
                conn.close()
                session['user'] = username
                return redirect(url_for('dashboard'))

    return render_template('login.html', form_type='login')


@app.route('/dashboard')
def dashboard():
    """Renders the main dashboard page."""
    if 'user' not in session:
        return redirect(url_for('auth'))
    return render_template('dashboard.html', username=session['user'])


@app.route('/logout')
def logout():
    """Logs the user out by clearing the session."""
    session.clear()
    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('auth'))


# -------------------------- Fleet Master Routes --------------------------

@app.route('/fleet_master', methods=['GET'])
def fleet_master():
    """Displays the fleet master data and handles filtering."""
    if 'user' not in session:

        session['user'] = 'Admin'

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM fleet ORDER BY vehicle_id")
    rows = cursor.fetchall()

    fleet_data = [{
        'vehicle_id': row[0],
        'vehicle_name': row[1],
        'make': row[2],
        'model': row[3],
        'vin': row[4],
        'type': row[5],
        'group': row[6],
        'status': row[7],
        'license_plate': row[8],
        'current_meter': row[9],
        'capacity_wei': row[10],
        'capacity_vol': row[11],
        'documents_expiry': row[12].strftime('%Y-%m-%d') if row[12] else '',
        'driver_id': row[13],
        'date_of_join': row[14].strftime('%Y-%m-%d') if row[14] else '',
        'avg': row[15] if row[15] is not None else 0
    } for row in rows]

    cursor.close()
    conn.close()

    return render_template('fleet_master.html', data=fleet_data, user=session['user'])


@app.route('/fleet_master/add', methods=['POST'])
def add_vehicle():
    """Adds a new vehicle to the fleet database."""
    form = request.form

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO fleet (
                vehicle_id, vehicle_name, make, model, vin, type, "group", status,
                license_plate, current_meter, capacity_weight_kg, capacity_vol_cbm,
                documents_expiry, driver_id, date_of_join, avg
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            form['vehicle_id'], form['vehicle_name'], form['make'], form['model'],
            form['vin'], form['type'], form['group'], form['status'],
            form['license_plate'], int(form['current_meter']),
            float(form['capacity_wei']), float(form['capacity_vol']),
            datetime.strptime(form['documents_expiry'], '%Y-%m-%d'),
            form['driver_id'],
            datetime.strptime(form['date_of_join'], '%Y-%m-%d'),
            float(form.get('avg') or 0)
        ))

        conn.commit()
        flash('Vehicle added successfully!', 'success')
    except psycopg2.IntegrityError:
        conn.rollback()
        flash('Vehicle ID already exists.', 'danger')
    except Exception as e:
        conn.rollback()
        flash(f'Error: {str(e)}', 'danger')
    finally:
        cursor.close()
        conn.close()

    return redirect('/fleet_master')


@app.route('/fleet_master/edit/<vehicle_id>', methods=['GET', 'POST'])
def edit_vehicle(vehicle_id):
    """Edits an existing vehicle's details."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == 'POST':
        form = request.form
        try:
            documents_expiry = form.get('documents_expiry')
            documents_expiry = datetime.strptime(documents_expiry, '%Y-%m-%d').date() if documents_expiry else None
            date_of_join = form.get('date_of_join')
            date_of_join = datetime.strptime(date_of_join, '%Y-%m-%d').date() if date_of_join else None

            cursor.execute("""
                UPDATE fleet
                SET vehicle_name = %s, driver_id = %s, make = %s, model = %s, vin = %s,
                    type = %s, "group" = %s, status = %s, license_plate = %s,
                    current_meter = %s, capacity_weight_kg = %s, capacity_vol_cbm = %s,
                    documents_expiry = %s, date_of_join = %s, avg = %s
                WHERE vehicle_id = %s
            """, (
                form.get('vehicle_name'), form.get('assigned_driver'), form.get('make'),
                form.get('model'), form.get('vin'), form.get('type'), form.get('group'),
                form.get('status'), form.get('license_plate'), int(form.get('current_meter') or 0),
                float(form.get('capacity_weight_kg') or 0), float(form.get('capacity_vol_cbm') or 0),
                documents_expiry, date_of_join, float(form.get('avg') or 0), vehicle_id
            ))

            conn.commit()
            flash('Vehicle updated successfully!', 'success')
            return redirect('/fleet_master')

        except Exception as e:
            conn.rollback()
            flash(f'Error updating vehicle: {str(e)}', 'danger')
            return redirect('/fleet_master')
        finally:
            cursor.close()
            conn.close()

    # GET method
    cursor.execute("SELECT * FROM fleet WHERE vehicle_id = %s", (vehicle_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        flash('Vehicle not found.', 'warning')
        return redirect('/fleet_master')

    vehicle_data = {
        'vehicle_id': row[0], 'vehicle_name': row[1], 'make': row[2],
        'model': row[3], 'vin': row[4], 'type': row[5], 'group': row[6],
        'status': row[7], 'license_plate': row[8], 'current_meter': row[9],
        'capacity_weight_kg': row[10], 'capacity_vol_cbm': row[11],
        'documents_expiry': row[12].strftime('%Y-%m-%d') if row[12] else '',
        'driver_id': row[13],
        'date_of_join': row[14].strftime('%Y-%m-%d') if row[14] else '',
        'avg': row[15] if row[15] is not None else 0
    }

    return render_template('edit_vehicle.html', vehicle=vehicle_data, user=session.get('user', ''))


# -------------------------- Driver Master Routes --------------------------

@app.route('/driver_master', methods=['GET', 'POST'])
def driver_master():
    """Manages driver data, including adding new drivers and displaying the list."""
    if 'user' not in session:
        return redirect('/')

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # --- Fetch fleet vehicle_ids for dropdown ---
    cur.execute("SELECT vehicle_id FROM fleet")
    fleet_rows = cur.fetchall()
    fleet_data = [row['vehicle_id'] for row in fleet_rows]

    if request.method == 'POST':
        form_data = request.form.to_dict()

        # Handle file uploads
        aadhar_file = request.files.get('aadhar_file')
        license_file = request.files.get('license_file')

        aadhar_filename = secure_filename(aadhar_file.filename) if aadhar_file and aadhar_file.filename else None
        license_filename = secure_filename(license_file.filename) if license_file and license_file.filename else None

        if aadhar_filename:
            aadhar_path = os.path.join(app.config['UPLOAD_FOLDER'], aadhar_filename)
            aadhar_file.save(aadhar_path)
        if license_filename:
            license_path = os.path.join(app.config['UPLOAD_FOLDER'], license_filename)
            license_file.save(license_path)

        try:
            # --- Insert into driver_master table including vehicle_id ---
            cur.execute("""
                INSERT INTO driver_master (
                    driver_id, driver_name, license_number, contact_number,
                    address, availability, shift_info, vehicle_id, aadhar_file, license_file
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                form_data['driver_id'], form_data['driver_name'], form_data['license_number'],
                form_data['contact_number'], form_data['address'], form_data['availability'],
                form_data['shift_info'], form_data['vehicle_id'], aadhar_filename, license_filename
            ))

            # Insert into driver_financials table (salary)
            salary = Decimal(form_data.get('salary', 0))
            cur.execute("""
                INSERT INTO driver_financials (driver_id, salary)
                VALUES (%s, %s)
            """, (form_data['driver_id'], salary))

            conn.commit()
            flash('Driver added successfully!', 'success')
        except psycopg2.IntegrityError:
            conn.rollback()
            flash('Driver ID already exists or a foreign key constraint failed.', 'danger')
        except Exception as e:
            conn.rollback()
            flash(f'An error occurred: {str(e)}', 'danger')

        return redirect(url_for('driver_master'))

    # Fetch all drivers using LEFT JOIN to include financial data
    cur.execute("""
        SELECT
            dm.driver_id,
            dm.driver_name,
            dm.license_number,
            dm.contact_number,
            dm.address,
            dm.availability,
            dm.shift_info,
            dm.vehicle_id,
            dm.aadhar_file,
            dm.license_file,
            df.salary
        FROM driver_master AS dm
        LEFT JOIN driver_financials AS df ON dm.driver_id = df.driver_id;
    """)

    data = cur.fetchall()
    cur.close()
    conn.close()

    return render_template('driver_master.html', data=data, fleet_data=fleet_data)



# -------------------------- Indent Management Routes --------------------------

def get_all_vehicle_numbers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT vehicle_id FROM fleet")   # 🔹 vehicle_id fetch
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # सिर्फ vehicle_id की list बनाएं
    return [row[0] for row in rows if row[0]]




def clean_numeric(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value




# 🔹 Update Vehicle Status
@app.route('/update_status', methods=['POST'])
def update_status():
    data = request.get_json()
    indent = data.get('indent')
    vehicle = data.get('vehicle')
    status = data.get('status')

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    conn = get_db_connection()
    cur = conn.cursor()

    if status == 'loading':
        cur.execute("""
            UPDATE indents
            SET loading_time = %s, status = %s
            WHERE indent = %s AND vehicle_number = %s
        """, (timestamp, 'loading', indent, vehicle))

    elif status == 'parking':
        cur.execute("""
            UPDATE indents
            SET parking_time = %s, status = %s
            WHERE indent = %s AND vehicle_number = %s
        """, (timestamp, 'parking', indent, vehicle))

    elif status == 'exit':
        cur.execute("""
            UPDATE indents
            SET exit_time = %s, status = %s
            WHERE indent = %s AND vehicle_number = %s
        """, (timestamp, 'exit', indent, vehicle))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({'success': True, 'timestamp': timestamp})


@app.route('/get_status')
def get_status():
    indent = request.args.get('indent')
    vehicle = request.args.get('vehicle')

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT loading_time, parking_time, exit_time, status
        FROM indents
        WHERE indent = %s AND vehicle_number = %s
        LIMIT 1
    """, (indent, vehicle))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return jsonify({
            'loading_time': row[0].strftime('%Y-%m-%d %H:%M:%S') if row[0] else None,
            'parking_time': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else None,
            'exit_time': row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else None,
            'status': row[3]
        })
    else:
        return jsonify({})




# --- Main Route Modification: Fetch Status Data ---
@app.route("/def", methods=["GET", "POST"])
def def_page():
    """Main route for displaying and adding indents."""
    valid_vehicles = get_all_vehicle_numbers()
    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == "POST":
        form_data = request.form.to_dict(flat=False)
        vehicle_no = request.form.get("vehicle_number", "").strip()

        if vehicle_no not in valid_vehicles:
            flash(f"Invalid Vehicle Number: {vehicle_no}", "danger")
            cursor.close()
            conn.close()
            return redirect(url_for("def_page"))

        try:
            # Existing logic for separating customers — unchanged
            customers = []
            for key, values in form_data.items():
                if key.startswith("customers["):
                    parts = key.split("[")
                    index = int(parts[1].replace("]", ""))
                    field = parts[2].replace("]", "")
                    while len(customers) <= index:
                        customers.append({})
                    # Data retrieval is made safer by providing "" as default
                    customers[index][field] = values[0] if values else ""

                    # Insert each customer indent
            for cust in customers:
                # ----------------------------------------------------------------
                # FIX: Using .get("key", "") for all form fields that expect strings
                # to prevent Python 'None' from causing string formatting errors.
                # ----------------------------------------------------------------

                # General Indent Fields
                indent_date = request.form.get("indent_date", "")
                indent_number = request.form.get("indent", "")
                allocation_date = request.form.get("allocation_date", "")
                pickup_location = request.form.get("pickup_location", "")
                vehicle_model = request.form.get("vehicle_model", "")
                vehicle_based = request.form.get("vehicle_based", "")
                pod_received = request.form.get("pod_received", "")
                ft_number = request.form.get("freight_tiger_number", "")
                ft_month = request.form.get("freight_tiger_month", "")

                # Customer Specific Fields
                cust_name = cust.get("name", "")
                cust_range = cust.get("range", "")
                drop_location = cust.get("drop_location", "")
                cust_lr_no = cust.get("lr_no", "")
                cust_material = cust.get("material", "")

                # Numeric Fields (clean_numeric handles the conversion, but safe retrieval is still good)
                load_per_bucket = clean_numeric(cust.get("load_per_bucket", None))
                no_of_buckets = clean_numeric(cust.get("no_of_buckets", None))
                total_load = clean_numeric(cust.get("total_load", None))

                cursor.execute("""
                    INSERT INTO indents (
                        indent_date, indent, allocation_date, customer_name, "range",
                        pickup_location, location, vehicle_number, vehicle_model,
                        vehicle_based, lr_no, material, load_per_bucket, no_of_buckets,
                        t_load, pod_received, freight_tiger_number, freight_tiger_month,
                        loading_time, parking_time, exit_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    indent_date,
                    indent_number,
                    allocation_date,
                    cust_name,
                    cust_range,
                    pickup_location,
                    drop_location,
                    vehicle_no,
                    vehicle_model,
                    vehicle_based,
                    cust_lr_no,
                    cust_material,
                    load_per_bucket,
                    no_of_buckets,
                    total_load,
                    pod_received,
                    ft_number,
                    ft_month,
                    None, None, None  # Tracking fields initialized as NULL (which is correct)
                ))

            conn.commit()
            flash(f"Indent created successfully with {len(customers)} customer(s)!", "success")

        except Exception as e:
            conn.rollback()
            # The error message now correctly shows the specific database error
            flash(f"Error creating indent: {str(e)}", "danger")

        cursor.close()
        conn.close()
        return redirect(url_for("def_page"))

    # --- Fetch all indents (showing new tracking fields) ---
    cursor.execute("SELECT * FROM indents ORDER BY indent_date DESC")
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    indent_data = [dict(zip(col_names, row)) for row in rows]
    indent_data = indent_data[:50]

    cursor.close()
    conn.close()

    return render_template("def.html", indent_data=indent_data, fleet_data=valid_vehicles)
# -------------------------- Upload Indents --------------------------

@app.route("/upload_indent", methods=["POST"])
def upload_indent():
    valid_vehicles = get_all_vehicle_numbers()
    file = request.files.get("file")

    if not file:
        flash("No file selected.", "danger")
        return redirect(url_for("def_page"))

    try:
        filename = file.filename
        if filename.endswith(".csv"):
            df = pd.read_csv(file)
        elif filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(file)
        else:
            flash("Unsupported file format.", "danger")
            return redirect(url_for("def_page"))

        df = df.where(pd.notnull(df), None)

        if df.empty:
            flash("Uploaded file is empty.", "warning")
            return redirect(url_for("def_page"))

        if 'vehicle_number' not in df.columns:
            flash("Missing 'vehicle_number' column.", "danger")
            return redirect(url_for("def_page"))

        valid_df = df[df["vehicle_number"].isin(valid_vehicles)]
        invalid_df = df[~df["vehicle_number"].isin(valid_vehicles)]

        conn = get_db_connection()
        cursor = conn.cursor()

        if not valid_df.empty:
            sql = """
                INSERT INTO indents (
                    indent_date, indent, allocation_date, customer_name, "range",
                    pickup_location, location, vehicle_number, vehicle_model,
                    vehicle_based, lr_no, material, load_per_bucket, no_of_buckets,
                    t_load, pod_received, freight_tiger_number, freight_tiger_month
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            for _, row in valid_df.iterrows():
                cursor.execute(sql, (
                    row.get("indent_date"),
                    row.get("indent"),
                    row.get("allocation_date"),
                    row.get("customer_name"),
                    row.get("range"),
                    row.get("pickup_location"),
                    row.get("location"),
                    row.get("vehicle_number"),
                    row.get("vehicle_model"),
                    row.get("vehicle_based"),
                    row.get("lr_no"),
                    row.get("material"),
                    clean_numeric(row.get("load_per_bucket")),
                    clean_numeric(row.get("no_of_buckets")),
                    clean_numeric(row.get("t_load")),
                    row.get("pod_received"),
                    row.get("freight_tiger_number"),
                    row.get("freight_tiger_month"),
                ))

            conn.commit()
            flash(f"{len(valid_df)} valid indent(s) uploaded successfully!", "success")

        if not invalid_df.empty:
            flash(f"{len(invalid_df)} row(s) skipped due to invalid vehicle numbers.", "warning")

        cursor.close()
        conn.close()

    except Exception as e:
        flash(f"Error processing file: {str(e)}", "danger")

    return redirect(url_for("def_page"))


# -------------------------- Export Indents --------------------------

@app.route("/export_indents")
def export_indents():
    """Exports all indents to a downloadable CSV file."""
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM indents", conn)
    conn.close()

    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    response = make_response(output.getvalue())
    response.headers["Content-Disposition"] = "attachment; filename=indents.csv"
    response.headers["Content-type"] = "text/csv"
    return response


# -------------------------- Master Model Routes --------------------------

@app.route("/master_model")
def master_model():
    """Displays the master model data."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM master_model")
    rows = cursor.fetchall()
    col_names = [desc[0] for desc in cursor.description]
    master_data = [dict(zip(col_names, row)) for row in rows]
    cursor.close()
    conn.close()
    return render_template("master_model.html", rows=master_data)


@app.route("/add", methods=["POST"])
def add_row():
    """Adds a new row to the master model table."""
    new_entry = {
        "range": request.form["range"],
        "product": request.form["product"],
        "transport_rate": request.form["transport_rate"],
        "loading_rate": request.form["loading_rate"],
        "unloading_rate": request.form["unloading_rate"],
        "modified_by": session.get('user', 'Admin')
    }

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO master_model (range, product, transport_rate, loading_rate, unloading_rate, modified_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        new_entry["range"], new_entry["product"], new_entry["transport_rate"],
        new_entry["loading_rate"], new_entry["unloading_rate"], new_entry["modified_by"]
    ))
    conn.commit()
    cursor.close()
    conn.close()

    flash("New business plan added successfully!", "success")
    return redirect(url_for("master_model"))


# -------------------------- Financial Dashboard Routes --------------------------

def calculate_distance(pickup, drop):
    """A placeholder function to calculate distance."""
    if (pickup == 'Sonipat' and drop == 'Delhi') or (pickup == 'Delhi' and drop == 'Sonipat'):
        return 80
    return 150

# ─────────────────────────────── Safe Decimal Helper ─────────────────────────────── #
# ─────────────── Helper: Safe Decimal ─────────────── #
def safe_decimal(value, default=0):
    """Safely convert any value to Decimal, returning default on failure."""
    try:
        if value in (None, '', 'NA', 'NaN'):
            return Decimal(default)
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


@app.route('/financial')
def financial():
    """Generates and displays the financial dashboard with filters, CSV export, and top 5 revenue vehicles."""
    vehicle_filter = request.args.get('vehicle_id', '').strip()
    driver_filter = request.args.get('driver_name', '').strip()
    export_csv = request.args.get('export', '').strip()
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # ─────────────── Constants ─────────────── #
    fuel_price_per_liter = safe_decimal('96.0')
    fuel_efficiency_kmpl = safe_decimal('12.0')
    toll_tax_per_trip = safe_decimal('100.0')
    misc_cost_per_trip = safe_decimal('50.0')
    cost_per_km_fuel = fuel_price_per_liter / fuel_efficiency_kmpl

    # ─────────────── Fetch all indents ─────────────── #
    query = """
    SELECT
        f.vehicle_id,
        f.vehicle_name,
        f.make,
        f.model,
        f.vin,
        f.avg AS vehicle_avg,
        f.status,
        f.type,
        f."group",
        dm.driver_name,
        i.indent_date,
        i.pickup_location,
        i.location AS drop_location,
        i.no_of_buckets,
        i.load_per_bucket,
        i.material,
        i.lr_no,
        i.customer_name,
        i.range
    FROM fleet f
    LEFT JOIN driver_master dm ON f.driver_id = dm.driver_id
    LEFT JOIN indents i ON f.vehicle_id::text = i.vehicle_number
    WHERE i.indent_date IS NOT NULL
    """

    # ─────────────── Dynamic filters ─────────────── #
    filters = []
    if vehicle_filter:
        filters.append(f"f.vehicle_id::text = '{vehicle_filter}'")
    if driver_filter:
        filters.append(f"dm.driver_name ILIKE '%{driver_filter}%'")
    if start_date and end_date:
        filters.append(f"i.indent_date BETWEEN '{start_date}' AND '{end_date}'")
    elif start_date:
        filters.append(f"i.indent_date >= '{start_date}'")
    elif end_date:
        filters.append(f"i.indent_date <= '{end_date}'")

    if filters:
        query += " AND " + " AND ".join(filters)

    cur.execute(query)
    rows = cur.fetchall()

    # ─────────────── SUM of End-of-Range Distance per Vehicle ─────────────── #
    range_query = """
        SELECT
            vehicle_number::text AS vehicle_id,
            SUM(
                COALESCE(
                    CASE
                        WHEN range ~ '^[0-9]+(\\.[0-9]+)?$' THEN range::numeric
                        WHEN range ~ '^[0-9]+(\\.[0-9]+)?-[0-9]+(\\.[0-9]+)?$' THEN
                            (regexp_replace(range, '.*-(\\d+(?:\\.\\d+)?)', '\\1'))::numeric
                        ELSE 0
                    END,
                    0
                )
            ) AS total_distance
        FROM indents
        WHERE range IS NOT NULL
    """

    # Apply same date filter for range aggregation
    if start_date and end_date:
        range_query += f" AND indent_date BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        range_query += f" AND indent_date >= '{start_date}'"
    elif end_date:
        range_query += f" AND indent_date <= '{end_date}'"

    range_query += " GROUP BY vehicle_number"

    cur.execute(range_query)
    range_data = {row['vehicle_id']: safe_decimal(row['total_distance']) for row in cur.fetchall()}

    cur.close()
    conn.close()

    # ─────────────── Calculations ─────────────── #
    from collections import defaultdict
    from decimal import Decimal

    routes = []
    summary = defaultdict(lambda: defaultdict(Decimal))

    for data in rows:
        no_of_buckets = safe_decimal(data.get('no_of_buckets'))
        load_per_bucket = safe_decimal(data.get('load_per_bucket'))
        vehicle_avg = safe_decimal(data.get('vehicle_avg'))
        total_km = safe_decimal(calculate_distance(data.get('pickup_location'), data.get('drop_location')))

        total_load = no_of_buckets * load_per_bucket
        revenue = total_load * vehicle_avg
        fuel_cost = total_km * cost_per_km_fuel
        total_cost = fuel_cost + toll_tax_per_trip + misc_cost_per_trip
        pnl = revenue - total_cost

        vehicle_id = data['vehicle_id']
        total_distance_travelled = range_data.get(str(vehicle_id), Decimal(0))

        trip = {
            'Vehicle ID': vehicle_id,
            'Vehicle Name': data.get('vehicle_name', ''),
            'Driver Name': data.get('driver_name', ''),
            'Status': data.get('status', ''),
            'Type': data.get('type', ''),
            'Group': data.get('group', ''),
            'Customer': data.get('customer_name', ''),
            'Material': data.get('material', ''),
            'LR No.': data.get('lr_no', ''),
            'Pickup': data.get('pickup_location', ''),
            'Drop': data.get('drop_location', ''),
            'No. Buckets': no_of_buckets,
            'Load/Bucket': load_per_bucket,
            'Total Load': total_load,
            'Fuel Cost': fuel_cost,
            'Total Cost': total_cost,
            'Revenue': revenue,
            'PnL': pnl,
            'Total Distance Travelled': total_distance_travelled
        }
        routes.append(trip)

        # Aggregate totals per vehicle
        summary[vehicle_id]['Vehicle ID'] = vehicle_id
        summary[vehicle_id]['Vehicle Name'] = data.get('vehicle_name', '')
        summary[vehicle_id]['Revenue'] += revenue
        summary[vehicle_id]['Fuel_Cost'] += fuel_cost
        summary[vehicle_id]['Total_Cost'] += total_cost
        summary[vehicle_id]['PnL'] += pnl
        summary[vehicle_id]['Trips'] += 1
        summary[vehicle_id]['Total Distance Travelled'] = total_distance_travelled

    # ─────────────── Top 5 Vehicles by Revenue ─────────────── #
    top_vehicles = sorted(summary.values(), key=lambda x: x['Revenue'], reverse=True)[:5]

    # ─────────────── CSV Export ─────────────── #
    if export_csv.lower() == 'csv':
        import pandas as pd, io
        df = pd.DataFrame(routes)
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=financial_trips.csv"
        response.headers["Content-type"] = "text/csv"
        return response

    # ─────────────── Render Template ─────────────── #
    return render_template(
        "financial_dashboard.html",
        routes=routes,
        summary=summary,
        top_vehicles=top_vehicles,
        vehicle_filter=vehicle_filter,
        driver_filter=driver_filter,
        start_date=start_date,
        end_date=end_date
    )


# -------------------------- Unused/Placeholder Routes --------------------------
# The following routes were in the original code but were incomplete or not
# connected to a database. They are kept here to maintain the original file
# structure but should be reviewed and implemented properly.

vehicles = []
service_records = []
vehicle_counter = 1
service_counter = 1


@app.route('/vehicle_maintenance')
def vehicle_maintenance():
    if 'user' not in session:
        return redirect('/')
    filters = {'vehicle_id': request.args.get('vehicle_id', '').strip(),
               'assigned_driver': request.args.get('assigned_driver', '').strip(),
               'status': request.args.get('status', '').strip()}
    filtered = vehicles
    if filters['vehicle_id']: filtered = [v for v in filtered if
                                          filters['vehicle_id'].lower() in v['vehicle_id'].lower()]
    if filters['assigned_driver']: filtered = [v for v in filtered if
                                               filters['assigned_driver'].lower() in v['assigned_driver'].lower()]
    if filters['status']: filtered = [v for v in filtered if v['status'] == filters['status']]
    return render_template('vehicle_maintenance.html', vehicles=filtered, filters=filters)


@app.route('/add_vehicle', methods=['GET', 'POST'])
def add_vehicle_form():
    global vehicle_counter
    if request.method == 'POST':
        data = request.form.to_dict()
        data['id'] = vehicle_counter
        data['service_cost'] = float(data.get('service_cost') or 0)
        data['last_service_date'] = datetime.strptime(data.get('last_service_date', ''), '%Y-%m-%d') if data.get(
            'last_service_date') else None
        data['next_service_due'] = datetime.strptime(data.get('next_service_due', ''), '%Y-%m-%d') if data.get(
            'next_service_due') else None
        vehicles.append(data)
        vehicle_counter += 1
        flash("Vehicle added successfully", "success")
        return redirect(url_for('vehicle_maintenance'))
    return render_template('add_vehicle.html')


@app.route('/add_service/<int:vehicle_id>', methods=['GET', 'POST'])
def add_service(vehicle_id):
    global service_counter
    vehicle = next((v for v in vehicles if v['id'] == vehicle_id), None)
    if not vehicle:
        flash("Vehicle not found", "danger")
        return redirect(url_for('vehicle_maintenance'))
    if request.method == 'POST':
        service = request.form.to_dict()
        service['id'] = service_counter
        service['vehicle_id'] = vehicle_id
        service['service_date'] = datetime.strptime(service.get('service_date'), '%Y-%m-%d')
        service['next_service_due'] = datetime.strptime(service.get('next_service_due'), '%Y-%m-%d')
        service['service_cost'] = float(service.get('service_cost') or 0)
        service_records.append(service)
        service_counter += 1
        vehicle['last_service_date'] = service['service_date']
        vehicle['next_service_due'] = service['next_service_due']
        vehicle['service_type'] = service.get('service_type')
        vehicle['status'] = service.get('status')
        vehicle['parts_replaced'] = service.get('parts_replaced')
        vehicle['service_cost'] = service['service_cost']
        vehicle['notes'] = service.get('notes')
        flash("Service added successfully", "success")
        return redirect(url_for('vehicle_maintenance'))
    return render_template('add_service.html', vehicle=vehicle)


@app.route('/delete_vehicle_men/<int:vehicle_id>', methods=['POST'])
def delete_vehicle_men(vehicle_id):
    global vehicles
    vehicles = [v for v in vehicles if v['id'] != vehicle_id]
    flash("Vehicle deleted successfully", "success")
    return redirect(url_for('vehicle_maintenance'))


tyres = []


@app.route('/tyre-management', methods=['GET', 'POST'])
def tyre_management():
    if 'user' not in session:
        return redirect('/')
    if request.method == 'POST':
        serial_number = request.form.get('serial_number')
        vehicle_id = request.form.get('vehicle_id')
        position = request.form.get('position')
        status = request.form.get('status')
        installed_on = request.form.get('installed_on')
        km_run = request.form.get('km_run')
        last_inspection = request.form.get('last_inspection')
        condition = request.form.get('condition')
        installed_on = datetime.strptime(installed_on, '%Y-%m-%d')
        last_inspection = datetime.strptime(last_inspection, '%Y-%m-%d')
        tyres.append({'serial_number': serial_number, 'vehicle_id': vehicle_id, 'position': position, 'status': status,
                      'installed_on': installed_on, 'km_run': int(km_run), 'last_inspection': last_inspection,
                      'condition': condition})
        flash('Tyre added successfully!', 'success')
        return redirect('/tyre-management')
    return render_template('tyre_management.html', tyres=tyres)


@app.route('/download_report')
def download_report():
    path = os.path.join('data/', 'trip_logs.csv')
    return send_file(path, as_attachment=True)

# --------- ORDER MANAGEMENT -----------
orders_data = []
@app.route('/orders', methods=['GET', 'POST'])
def orders():
    if 'user' not in session:
        return redirect('/')

    conn = get_db_connection()
    cur = conn.cursor()

    if request.method == 'POST':
        data = request.form
        order_id = data['order_id']

        # Check if order exists
        cur.execute("SELECT 1 FROM orders WHERE order_id = %s", (order_id,))
        exists = cur.fetchone()

        if exists:
            # Update existing order
            cur.execute("""
                UPDATE orders SET
                    customer_name = %s,
                    created_date  = %s,
                    order_type = %s,
                    pickup_location_latlon = %s,
                    drop_location_latlon = %s,
                    volume_cbm = %s,
                    weight_kg = %s,
                    delivery_priority = %s,
                    expected_delivery = %s,
                    amount = %s,
                    status = %s
                WHERE order_id = %s
            """, (
                data['customer_name'],
                data['created_date'],
                data['order_type'],
                data['pickup_location_latlon'],
                data['drop_location_latlon'],
                data['volume_cbm'],
                data['weight_kg'],
                data['delivery_priority'],
                data['expected_delivery'],
                data['amount'],
                data['status'],
                order_id
            ))
        else:
            # Insert new order
            cur.execute("""
                INSERT INTO orders (
                    order_id, customer_name, created_date, order_type, pickup_location_latlon,
                    drop_location_latlon, volume_cbm, weight_kg,
                    delivery_priority, expected_delivery, amount, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['order_id'],
                data['customer_name'],
                data['created_date'],
                data['order_type'],
                data['pickup_location_latlon'],
                data['drop_location_latlon'],
                data['volume_cbm'],
                data['weight_kg'],
                data['delivery_priority'],
                data['expected_delivery'],
                data['amount'],
                data['status']
            ))

        conn.commit()

    # Fetch all orders
    cur.execute("SELECT * FROM orders ORDER BY expected_delivery")
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    data = [dict(zip(colnames, row)) for row in rows]

    cur.close()
    conn.close()

    return render_template('orders.html', data=data)


@app.route('/delete_order/<order_id>', methods=['POST'])
def delete_order(order_id):
    if 'user' not in session:
        return redirect('/')

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error deleting order:", e)
    finally:
        cur.close()
        conn.close()

    return redirect('/orders')
@app.route('/upload_orders', methods=['POST'])
def upload_orders():
    if 'user' not in session:
        return redirect('/')

    file = request.files['orders_file']
    if file and file.filename.endswith('.csv'):
        import pandas as pd
        df = pd.read_csv(file)
        conn = get_db_connection()
        cur = conn.cursor()

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO orders (
                    order_id, customer_name, created_date, order_type, pickup_location_latlon,
                    drop_location_latlon, volume_cbm, weight_kg,
                    delivery_priority, expected_delivery, amount, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    created_date = EXCLUDED.created_date,
                    order_type = EXCLUDED.order_type,
                    pickup_location_latlon = EXCLUDED.pickup_location_latlon,
                    drop_location_latlon = EXCLUDED.drop_location_latlon,
                    volume_cbm = EXCLUDED.volume_cbm,
                    weight_kg = EXCLUDED.weight_kg,
                    delivery_priority = EXCLUDED.delivery_priority,
                    expected_delivery = EXCLUDED.expected_delivery,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status
            """, (
                row['Order_ID'], row['Customer_Name'], row['created_date'], row['Order_Type'],
                row['Pickup_Location_LatLon'], row['Drop_Location_LatLon'],
                row['Volume_CBM'], row['Weight_KG'],
                row['Delivery_Priority'], row['Expected_Delivery'], row['amount'],
                row['Status']
            ))

        conn.commit()
        cur.close()
        conn.close()

    return redirect('/orders')

# ---------------- EDIT (Pre-fill Form) ----------------
@app.route('/edit_order/<order_id>')
def edit_order(order_id):
    if 'user' not in session:
        return redirect('/')

    order = next((o for o in orders_data if o['Order_ID'] == order_id), None)
    if not order:
        return redirect('/orders')
    return render_template('orders.html', data=orders_data, edit_order=order)





from flask import Flask, render_template, request, flash, redirect
from urllib.parse import quote_plus
import requests, time, math
from ortools.constraint_solver import routing_enums_pb2, pywrapcp
import pytz

# ------------------ Utility Functions ------------------

# -------- Utility Functions --------
# -------- Utility Functions --------
def geocode_address(address, retries=3, delay=1.0):
    """
    Geocode using Nominatim (OpenStreetMap) with retries.
    Returns (lat, lon) or (None, None).
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json", "limit": 1}
    headers = {"User-Agent": "MyApp/1.0 (your-email@example.com)"}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                lat = float(data[0].get("lat"))
                lon = float(data[0].get("lon"))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return lat, lon
            app.logger.debug(f"Geocode attempt {attempt} returned no data for '{address}'")
        except Exception as e:
            app.logger.debug(f"Geocode attempt {attempt} failed for '{address}': {e}")
        time.sleep(delay)
    app.logger.warning(f"Geocode failed for address: {address}")
    return None, None

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def create_distance_matrix(coords):
    """
    coords: list of (lat, lon) tuples; entries may be (None, None)
    Returns n x n matrix of distances in km (float). If either coord invalid, distance is 0.0.
    """
    n = len(coords)
    matrix = [[0.0] * n for _ in range(n)]
    invalid_indices = [i for i, (lat, lon) in enumerate(coords) if lat is None or lon is None]
    if invalid_indices:
        app.logger.warning(f"create_distance_matrix: invalid coordinates at indices {invalid_indices}")
    for i in range(n):
        lat1, lon1 = coords[i]
        for j in range(n):
            if i == j:
                matrix[i][j] = 0.0
                continue
            lat2, lon2 = coords[j]
            if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
                matrix[i][j] = 0.0
            else:
                try:
                    matrix[i][j] = haversine(lat1, lon1, lat2, lon2)
                except Exception as e:
                    app.logger.error(f"haversine error for {i}->{j}: {e}")
                    matrix[i][j] = 0.0
    # Debug print
    rows_str = "\n".join([", ".join(f"{v:.3f}" for v in row) for row in matrix])
    app.logger.debug(f"Distance matrix:\n{rows_str}")
    return matrix

def solve_tsp(distance_matrix):
    n = len(distance_matrix)
    if n < 2:
        app.logger.warning("solve_tsp called with less than 2 nodes")
        return None
    manager = pywrapcp.RoutingIndexManager(n, 1, 0)
    routing = pywrapcp.RoutingModel(manager)

    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        val = distance_matrix[from_node][to_node]
        # integer meters for OR-Tools
        return int(val * 1000)

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    try:
        solution = routing.SolveWithParameters(search_parameters)
    except Exception as e:
        app.logger.error(f"OR-Tools error: {e}")
        return None

    if solution:
        index = routing.Start(0)
        route = []
        while not routing.IsEnd(index):
            route.append(manager.IndexToNode(index))
            index = solution.Value(routing.NextVar(index))
        route.append(manager.IndexToNode(index))
        app.logger.debug(f"solve_tsp route indices: {route}")
        if len(route) <= 1:
            return None
        return route
    app.logger.warning("solve_tsp: no solution")
    return None

def calculate_total_distance(route_index, distance_matrix):
    if not route_index or len(route_index) < 2:
        return 0.0
    total_distance = 0.0
    for i in range(len(route_index) - 1):
        a = route_index[i]
        b = route_index[i+1]
        try:
            total_distance += float(distance_matrix[a][b])
        except Exception as e:
            app.logger.error(f"calculate_total_distance error for {a}->{b}: {e}")
    return total_distance

def create_google_maps_url(address_list):
    from urllib.parse import quote_plus
    parts = [quote_plus(a) for a in address_list]
    return "https://www.google.com/maps/dir/" + "/".join(parts) + "/"

def save_trip_to_db(indent_id, vehicle, pickup, drops, total_distance, est_arrival, exit_time):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        drop_location = ", ".join(drops)
        total_drops = len(drops)
        duration_hours = round(total_distance / 40, 2) if total_distance else 0.0
        cursor.execute("""
            INSERT INTO trip_data (
                indent_id, vehicle_no, driver_name, pickup, drop_location,
                total_drops, exit_time, eta_arrival_time, actual_arrival_time,
                total_distance, duration_hours, customer_details, pod_url, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            indent_id,
            vehicle,
            "AUTO-DRIVER",
            pickup,
            drop_location,
            total_drops,
            exit_time,
            str(est_arrival),
            None,
            total_distance,
            duration_hours,
            "[]",
            None
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        app.logger.error(f"save_trip_to_db failed: {e}")

# ---------------- Optimize route ----------------

@app.route('/optimize', methods=["GET", "POST"])
def optimize():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        cursor.execute("""
            SELECT indent, vehicle_number, pickup_location, location, exit_time
            FROM indents
            WHERE DATE(indent_date) BETWEEN %s AND %s
            ORDER BY indent_date DESC
            LIMIT 5
        """, (yesterday, today))

        rows = cursor.fetchall()
        indents_data = []
        ist = pytz.timezone('Asia/Kolkata')

        for row in rows:
            indent_id, vehicle, pickup, drops_raw, exit_time = row
            drops = [d.strip() for d in drops_raw.split(",") if d.strip()]
            exit_time = exit_time or datetime.now()
            exit_time_ist = exit_time.astimezone(ist)

            all_addresses = [pickup] + drops

            app.logger.debug(f"Indent {indent_id} addresses: {all_addresses}")

            # Geocode addresses (retry internally)
            coords = []
            for addr in all_addresses:
                lat, lon = geocode_address(addr)
                # If geocode failed, log and leave (None, None)
                if lat is None or lon is None:
                    app.logger.warning(f"Geocode missing for '{addr}' (indent {indent_id})")
                    coords.append((None, None))
                else:
                    coords.append((lat, lon))
                # Be polite to Nominatim
                time.sleep(1)

            app.logger.debug(f"Indent {indent_id} coords: {coords}")

            # Create distance matrix
            dist_matrix = create_distance_matrix(coords)

            # If too many invalid coords, skip TSP and default to input order
            invalid_count = sum(1 for (lat, lon) in coords if lat is None or lon is None)
            if invalid_count > 0 and invalid_count >= len(coords) - 1:
                app.logger.warning(f"Too many invalid coords for indent {indent_id}, skipping TSP")
                auto_route = all_addresses
                total_distance = 0.0
                est_arrival = [exit_time_ist] * len(all_addresses)
            else:
                auto_route_index = solve_tsp(dist_matrix)
                if auto_route_index:
                    auto_route = [all_addresses[i] for i in auto_route_index]
                    total_distance = round(calculate_total_distance(auto_route_index, dist_matrix), 2)

                    # Build cumulative ETA list aligned with auto_route
                    est_arrival = [exit_time_ist]  # pickup first
                    cumulative_hours = 0.0
                    # iterate pairs over the route indices
                    for j in range(len(auto_route_index)-1):
                        from_idx = auto_route_index[j]
                        to_idx = auto_route_index[j+1]
                        dist_km = dist_matrix[from_idx][to_idx]
                        travel_hours = (dist_km / 40.0) if dist_km else 0.0
                        cumulative_hours += travel_hours
                        eta = exit_time + timedelta(hours=cumulative_hours)
                        eta_ist = eta.astimezone(ist)
                        est_arrival.append(eta_ist)
                else:
                    # fallback: no TSP solution
                    app.logger.warning(f"No TSP route for indent {indent_id}; using input order")
                    auto_route = all_addresses
                    total_distance = 0.0
                    est_arrival = [exit_time_ist] * len(all_addresses)

            app.logger.info(f"Indent {indent_id} total_distance = {total_distance} km; route len={len(auto_route)}")

            # Save single trip summary
            save_trip_to_db(
                indent_id=indent_id,
                vehicle=vehicle,
                pickup=pickup,
                drops=drops,
                total_distance=total_distance,
                est_arrival=est_arrival[-1] if est_arrival else exit_time_ist,
                exit_time=exit_time
            )

            indents_data.append({
                "indent_id": indent_id,
                "vehicle": vehicle,
                "pickup": pickup,
                "auto_route": auto_route,
                "total_distance": total_distance,
                "estimated_arrival": est_arrival,
                "map_url": create_google_maps_url(all_addresses),
                "exit_time": exit_time_ist,
            })

        cursor.close()
        conn.close()
        return render_template("route_optimize.html", indents_data=indents_data)

    except Exception as e:
        app.logger.error(f"/optimize error: {e}")
        return "Internal Server Error", 500


# -------- Trip History --------


@app.route('/trip-history', methods=['GET'])
def trip_history():
    # Filters
    vehicle_filter = request.args.get('vehicle')
    indent_filter = request.args.get('indent')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    conn = get_db_connection()
    cursor = conn.cursor()

    # Main query: fetch trips with customer info
    query = """
        SELECT
            t.indent_id, t.vehicle_no, t.driver_name,
            t.pickup, t.drop_location, t.total_drops,
            t.exit_time, t.eta_arrival_time, t.actual_arrival_time,
            t.total_distance, t.duration_hours, t.customer_details,
            t.pod_url, t.created_at, i.customer_name
        FROM trip_data t
        LEFT JOIN indents i
            ON t.indent_id = i.indent
        WHERE 1=1
    """
    params = []

    if vehicle_filter:
        query += " AND t.vehicle_no=%s"
        params.append(vehicle_filter)
    if indent_filter:
        query += " AND t.indent_id=%s"
        params.append(indent_filter)
    if start_date and end_date:
        query += " AND DATE(t.exit_time) BETWEEN %s AND %s"
        params.extend([start_date, end_date])

    query += " ORDER BY t.indent_id, t.id DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    trips_dict = {}  # Use dict to store only one entry per indent_id

    for r in rows:
        indent_id = r[0]

        if indent_id in trips_dict:
            continue  # Already added this indent_id

        status = "Pending"  # default
        actual = r[8]  # actual_arrival_time
        eta = r[7]  # eta_arrival_time

        if actual:
            if eta:
                if actual == eta:
                    status = "On Time"
                elif actual < eta:
                    status = "Arrived"
                else:
                    status = "Delayed"
            else:
                status = "Arrived"

        # Customer name logic
        customer_list = []
        if r[14]:  # i.customer_name exists
            customer_list.append({"name": r[14]})
        elif r[11]:  # customer_details JSON
            try:
                if isinstance(r[11], str):
                    customer_list = json.loads(r[11])
                else:
                    customer_list = r[11]
            except Exception:
                customer_list = []

        trips_dict[indent_id] = {
            "indent_id": r[0],
            "vehicle_no": r[1],
            "driver_name": r[2] if r[2] else None,
            "pickup": r[3],
            "drop_location": r[4],
            "total_drops": r[5],
            "exit_time": r[6],
            "eta_arrival_time": r[7],
            "actual_arrival_time": r[8],
            "total_distance": r[9],
            "duration_hours": r[10],
            "customer_details": customer_list,
            "pod_url": r[12],
            "created_at": r[13],
            "status": status
        }

    cursor.close()
    conn.close()

    # Convert dict values to list for template
    trips = list(trips_dict.values())

    return render_template("trip_history.html", trips=trips)



@app.route('/update-trip', methods=['POST'])
def update_trip():
    data = request.json
    trip_id = data.get("trip_id")
    actual_arrival_time = data.get("actual_arrival_time")
    pod_url = data.get("pod_url")

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE trip_data
        SET actual_arrival_time=%s,
            pod_url=%s,
            updated_at=NOW()
        WHERE id=%s
    """, (actual_arrival_time, pod_url, trip_id))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({"status": "success", "message": "Trip updated successfully"})

@app.route('/tracking')
def tracking():
    return render_template('tracking.html')

if __name__ == '__main__':
    app.run(debug=True)
