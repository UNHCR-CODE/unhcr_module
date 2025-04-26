
from datetime import date, datetime
import time
from flask import (
    Flask,
    has_request_context,
    render_template,
    render_template_string,
    request,
    redirect,
    flash,
    Response,
    session as flask_session,
    url_for
)

from flask_babel import Babel
from flask_sqlalchemy import SQLAlchemy
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from sqlalchemy import (
    create_engine,
    func,
    inspect,
    MetaData,
    Table,
    PrimaryKeyConstraint,
    Column,
    Boolean, BigInteger, Integer, String, DateTime, Float, Date,
    and_, or_,
    text
)
from sqlalchemy.dialects import postgresql
from wtforms import StringField, IntegerField, BooleanField, DateField
from wtforms.validators import DataRequired
from wtforms.widgets import Input

from flask_wtf import FlaskForm
from dotenv import load_dotenv
import os
from pathlib import Path
import uuid
from sqlalchemy.ext.declarative import declarative_base

PASSWORD = "Unhcr.007"  # Replace with your desired password

# Load .env
project_root = Path(__file__).parent.resolve()
load_dotenv(dotenv_path=project_root / ".env")

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev")
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

Base = declarative_base()
db = SQLAlchemy(app)
babel = Babel(app)

engine = create_engine(app.config["SQLALCHEMY_DATABASE_URI"])
inspector = inspect(engine)

ALLOWED_SCHEMAS = {'eyedro','solarman','public'}
model_registry = {}


# Define the widget to make the input readonly with custom styling
# Define the widget to make the input readonly with custom styling
class ReadOnlyInput(Input):
    input_type = 'text'

    def __call__(self, field, **kwargs):
        kwargs['disabled'] = True  # Ensure the field is disabled
        kwargs['style'] = 'background-color: #f0f0f0; color: #999; cursor: not-allowed; border: 2px solid red;'  # Add a visible grey background
        kwargs['title'] = 'This field is read-only (PK or FK)'

        # Call the parent class's __call__ method correctly using Input's __call__
        return Input.__call__(self, field, **kwargs) 

class DynamicRowModel(db.Model):
    __abstract__ = True  # Not meant to be instantiated directly.

    def __init__(self, row):
        # Dynamically set attributes based on the row's columns
        for key, value in row.items():
            setattr(self, key, value)  # Assign each column to the instance's attributes


    def __repr__(self):
        return f"<DynamicRowModel {self.__dict__}>"


# Dummy model
class DummyModel(Base):
    __tablename__ = "dummy"
    id = Column(Integer, primary_key=True)


def get_pagination_data(self):
    model = self.get_model()
    if not model:
        return None
    
    row_count = self.get_row_count()
    use_offset_pagination = row_count <= 1_000_000
    pagination_data = {}

    if use_offset_pagination:
        # Traditional offset-based pagination
        page = request.args.get('page', 1, type=int)
        pagination_data = {
            'page': page,
            'page_size': self.page_size,
            'total_pages': (row_count // self.page_size) + (1 if row_count % self.page_size else 0),
            'has_prev': page > 1,
            'has_next': page * self.page_size < row_count
        }
    else:
        # Keyset pagination
        after = request.args.get('after', None, type=int)
        pagination_data = {
            'after': after,
            'page': page,
            'page_size': self.page_size,
            'before': None,  # Add backward pagination if necessary
            'has_more': row_count > (self.page_size if after else 0)
        }

    return pagination_data



def create_model_for_table(schema, table_name):
    key = f"{schema}.{table_name}"
    if key in model_registry:
        return model_registry[key]

    metadata = MetaData()
    try:
        table = Table(
            table_name,
            metadata,
            schema=schema,
            autoload_with=engine,
            extend_existing=True,
        )
    except Exception as e:
        print(f"Failed to load table {schema}.{table_name}: {e}")
        raise ValueError(f"Cannot load table {schema}.{table_name}: {e}")

    # Check if PK exists
    has_pk = table.primary_key and len(table.primary_key.columns) > 0
    if not has_pk:
        print(
            f"⚠️ No primary key found for {schema}.{table_name}, applying fallback logic..."
        )

        # Try to find a suitable fallback column (unique, not nullable)
        fallback_col = None

        # First pass: try unique or not nullable column
        for col in table.columns:
            if col.unique or not col.nullable:
                fallback_col = col
                break

        # Second pass: just pick the first column
        if not fallback_col:
            fallback_col = list(table.columns)[0]

        if fallback_col is not None:
            print(f"ℹ️ Assigning fallback PK: {fallback_col.name}")
            table.append_constraint(PrimaryKeyConstraint(fallback_col))
        else:
            raise ValueError(
                f"🚫 Unable to determine a fallback PK for table: {schema}.{table_name}"
            )

    class_name = f"{schema}_{table_name}_{uuid.uuid4().hex[:8]}"
    model_class = type(
        class_name,
        (db.Model,),
        {
            "__table__": table,
            "__tablename__": table_name,
            "__table_args__": {"schema": schema, "extend_existing": True},
        },
    )

    model_registry[key] = model_class
    return model_class


class DynamicTableView(ModelView):
    can_delete = True
    can_edit = True
    can_create = True
    column_display_pk = True
    page_size = 10  # Rows per page
    list_template = 'admin/custom_list.html'

    @property
    def primary_key_columns(self):
        return [col.name for col in self.model.__table__.primary_key.columns]
    
    def __init__(self, session, name="DynamicTable", endpoint="dynamictable", url="/admin/dynamictable", **kwargs):
        self.session = session
        self._is_initializing = True  # Initialize to True during object creation
        kwargs.setdefault("endpoint", endpoint)
        kwargs.setdefault("url", url)
        super().__init__(DummyModel, session, name=name, **kwargs)
        self._is_initializing = False  # Set to False after initialization

    def get_model(self):
        """Return the model based on the selected schema and table."""
        if not has_request_context():
            return None
        schema = flask_session.get("schema")
        table = flask_session.get("table")
        if not schema or not table:
            return None
        try:
            return create_model_for_table(schema, table)
        except Exception as e:
            flash(f"Error loading table: {str(e)}")
            return None

    def parse_date(self, date_str):
        # Common date formats to try
        formats = [
        "%Y-%m-%d %H:%M:%S",       # ISO 8601 (YYYY-MM-DD HH:MM:SS)
        "%Y-%m-%d %H:%M",           # ISO 8601 without seconds (YYYY-MM-DD HH:MM)
        "%Y-%m-%d",                 # Date only (YYYY-MM-DD)
        "%m/%d/%Y %H:%M:%S",        # US style with time (MM/DD/YYYY HH:MM:SS)
        "%m/%d/%Y %H:%M",           # US style with time (MM/DD/YYYY HH:MM)
        "%m/%d/%Y",                 # US style date only (MM/DD/YYYY)
        "%d-%m-%Y %H:%M:%S",        # EU style with time (DD-MM-YYYY HH:MM:SS)
        "%d-%m-%Y %H:%M",           # EU style with time (DD-MM-YYYY HH:MM)
        "%d-%m-%Y",                 # EU style date only (DD-MM-YYYY)
        "%Y/%m/%d %H:%M:%S",        # ISO-like with slashes (YYYY/MM/DD HH:MM:SS)
        "%Y/%m/%d %H:%M",           # ISO-like with slashes (YYYY/MM/DD HH:MM)
        "%Y%m%d%H%M%S",             # Compact ISO (YYYYMMDDHHMMSS)
        "%Y%m%d",                   # Compact ISO date (YYYYMMDD)
        "%I:%M %p",                 # US-style time (12-hour format with AM/PM)
        "%I:%M:%S %p",              # US-style time with seconds (12-hour format with AM/PM)
        "%m/%d/%Y %I:%M %p",        # US with AM/PM (MM/DD/YYYY HH:MM AM/PM)
        "%m/%d/%Y %I:%M:%S %p",     # US with AM/PM (MM/DD/YYYY HH:MM:SS AM/PM)
        "%d.%m.%Y",                 # EU/Asia style with dots (DD.MM.YYYY)
        "%d.%m.%Y %H:%M:%S",        # EU/Asia style with dots and time (DD.MM.YYYY HH:MM:SS)
        "%d.%m.%Y %H:%M",           # EU/Asia style with dots and time (DD.MM.YYYY HH:MM)
        "%b %d, %Y",                # Month abbreviation with date (e.g., "Oct 01, 2024")
        "%B %d, %Y %H:%M",          # Full month name with time (e.g., "October 01, 2024 14:30")
        "%B %d, %Y",                # Full month name (e.g., "October 01, 2024")
        ]
        
        for fmt in formats:
            try:
                res = datetime.strptime(date_str, fmt)
                if res == '':
                    continue
                return res
            except ValueError:
                continue
        return None

    def get_filters_from_request(self, request, model):
        # Apply filters from request parameters
        combined_filter = None
        start_dt = request.args.get('start_date')
        end_dt = request.args.get('end_date')
        date_col = request.args.get('date_column')
        column_name = date_col
        condition = None
        if column_name and hasattr(model, column_name):
            column = getattr(model, column_name)
            if hasattr(column.type, 'python_type'):
                python_type = column.type.python_type
                column = getattr(model, column_name)
                if issubclass(python_type, (datetime, str)):
                    start_date = self.parse_date(start_dt)
                    end_date = self.parse_date(end_dt)
                    condition = None
                    if start_date == end_date == None:
                        condition = None
                    elif start_date and end_date:
                        if start_date > end_date:
                            flash("Start date must be less than or equal to end date.")
                        else:
                            condition = column.cast(Date).between(start_date, end_date)
                    elif start_date is None and end_date is not None:
                        condition = column.cast(Date) <= end_date
                    elif start_date is not None and end_date is None:
                        condition = column.cast(Date) >= start_date
                else: # assume str
                    print(f"Date column type: {python_type} not date or str")
                if condition is not None:
                    combined_filter = condition
            else:
                flash("Date column type not found")
        else:
            print(f'Date column not found "{date_col}"')

        for i in range(10):  # Limit to reasonable number of filters
            column_name = request.args.get(f'filter_column_{i}')
            filter_value = request.args.get(f'filter_value_{i}')
            comparison_op = request.args.get(f'filter_operator_{i}', '=')  # This is =, !=, >, <, etc.
            logical_op = request.args.get(f'filter_logical_{i}', 'AND')  # This is AND or OR

            # Break if no more filters
            if not column_name or filter_value is None:
                break

            # Check if column exists in model
            if hasattr(model, column_name):
                column = getattr(model, column_name)
                # Create filter condition based on operator
                try:
                    # Handle NULL and NOT NULL cases
                    if comparison_op == 'is null':
                        condition = column.is_(None)
                    elif comparison_op == 'is not null':
                        condition = column.isnot(None)
                    else:
                        # Type conversion for non-null operators
                        if hasattr(column.type, 'python_type'):
                            python_type = column.type.python_type
                            # Handle different operators based on data type
                            if issubclass(python_type, (int, float, datetime)):
                                if issubclass(python_type, (datetime)):
                                    dt = self.parse_date(filter_value)
                                    if dt:
                                        filter_value = dt
                                    else:
                                        print(f"DEBUG: Invalid date format {filter_value}")
                                        continue
                                try:
                                    filter_value = python_type(filter_value)
                                    if comparison_op == '=':
                                        condition = column == filter_value
                                    elif comparison_op == '!=':
                                        condition = column != filter_value
                                    elif comparison_op == '>':
                                        condition = column > filter_value
                                    elif comparison_op == '<':
                                        condition = column < filter_value
                                    elif comparison_op == '>=':
                                        condition = column >= filter_value
                                    elif comparison_op == '<=':
                                        condition = column <= filter_value
                                    else:
                                        # Default to equality
                                        condition = column == filter_value
                                except ValueError:
                                    print(f"DEBUG: Value conversion error for {filter_value}")
                                    continue
                            elif issubclass(python_type, str):
                                if comparison_op == 'ilike':
                                    condition = column.ilike(f'%{filter_value}%')
                                elif comparison_op == '=':
                                    condition = column == filter_value
                                elif comparison_op == '!=':
                                    condition = column != filter_value
                                elif comparison_op == 'in':
                                    # Split comma-separated values and strip whitespace
                                    values = [v.strip() for v in filter_value.split(',')]
                                    condition = column.in_(values)
                                elif comparison_op == 'not in':
                                    values = [v.strip() for v in filter_value.split(',')]
                                    condition = ~column.in_(values)
                                else:
                                    # Default to contains for string
                                    condition = column.ilike(f'%{filter_value}%')
                            else:
                                # For other types, use basic operators
                                if comparison_op == '=':
                                    condition = column == filter_value
                                elif comparison_op == '!=':
                                    condition = column != filter_value
                                else:
                                    # Default to equality
                                    condition = column == filter_value
                        else:
                            # For columns without specific type info, default to ilike for strings
                            if comparison_op == 'ilike':
                                condition = column.ilike(f'%{filter_value}%')
                            elif comparison_op == '=':
                                condition = column == filter_value
                            elif comparison_op == '!=':
                                condition = column != filter_value
                            else:
                                # Default behavior
                                condition = column.ilike(f'%{filter_value}%')

                    # Build combined filter condition
                    if combined_filter is None:
                        combined_filter = condition
                    elif i > 0 and logical_op == 'OR':
                        combined_filter = or_(combined_filter, condition)
                    else:  # Default to AND
                        combined_filter = and_(combined_filter, condition)
                    print(f"DEBUG: Added filter: {column_name} {comparison_op} {filter_value} with logical operator {logical_op if i > 0 else 'FIRST'}")
                except Exception as e:
                    print(f"DEBUG: Error applying filter: {str(e)}")
            else:
                print(f"DEBUG: Invalid filter column: {column_name}")

        return combined_filter


    def get_list(self, page, sort_column, sort_desc, search, filters, execute=True, page_size=None):
        """Overridden get_list method with filter support."""
        model = self.get_model()
        if not model:
            return 0, []  # Return an empty list if no model is found.

        # Build base query
        query = self.session.query(model)
        self.count_query = self.session.query(func.count()).select_from(model)
        count = self.count_query.scalar()
        # # Apply filters from request parameters
        # combined_filter = None
        # start_dt = request.args.get('start_date')
        # end_dt = request.args.get('end_date')
        # date_col = request.args.get('date_column')
        # column_name = date_col
        # if column_name and hasattr(model, column_name):
        #     column = getattr(model, column_name)
        #     if hasattr(column.type, 'python_type'):
        #         condition = None
        #         python_type = column.type.python_type
        #         if issubclass(python_type, (datetime, str)):
        #             start_date = self.parse_date(start_dt)
        #             end_date = self.parse_date(end_dt)
        #             if start_date == end_date == None:
        #                 condition = None
        #             elif start_date and end_date:
        #                 condition = column.cast(Date).between(start_date, end_date)
        #             elif start_date is None and end_date is not None:
        #                 condition = column.cast(Date) <= end_date
        #             elif start_date is not None and end_date is None:
        #                 condition = column.cast(Date) >= start_date
        #         else: # assume str
        #             print(f"Date column type: {python_type} not date or str")
        #         if condition is not None:
        #             combined_filter = condition
        #     else:
        #         print("Date column type not found")
        # else:
        #     print("Date column not found")

        # if has_request_context():
        #     for i in range(10):  # Limit to reasonable number of filters
        #         column_name = request.args.get(f'filter_column_{i}')
        #         filter_value = request.args.get(f'filter_value_{i}')
        #         operator = request.args.get(f'filter_operator_{i}', 'AND')

        #         if not column_name or not filter_value:
        #             break

        #         if hasattr(model, column_name):
        #             column = getattr(model, column_name)
        #             # Create filter condition
        #             try:
        #                 if hasattr(column.type, 'python_type'):
        #                     python_type = column.type.python_type

        #                     if issubclass(python_type, (int, float)):
        #                         try:
        #                             filter_value = python_type(filter_value)
        #                             condition = column == filter_value
        #                         except ValueError:
        #                             continue
        #                     elif issubclass(python_type, str):
        #                         condition = column.ilike(f'%{filter_value}%')
        #                     else:
        #                         condition = column == filter_value
        #                 else:
        #                     condition = column.ilike(f'%{filter_value}%')
        #                 # Build combined filter condition
        #                 if combined_filter is None:
        #                     combined_filter = condition
        #                 elif operator == 'OR':
        #                     combined_filter = or_(combined_filter, condition)
        #                 else:  # AND
        #                     combined_filter = and_(combined_filter, condition)

        #                 print(f"DEBUG: Added filter: {column_name} {'LIKE' if isinstance(condition, str) else '='} {filter_value} with {operator}")
        #             except Exception as e:
        #                 print(f"DEBUG: Error applying filter: {str(e)}")
        #         else:
        #             print(f"DEBUG: Invalid filter column: {column_name}")

        # #####combined_filter = None
        # if has_request_context():
        #     for i in range(10):  # Limit to reasonable number of filters
        #         column_name = request.args.get(f'filter_column_{i}')
        #         filter_value = request.args.get(f'filter_value_{i}')
        #         logical_operator = request.args.get(f'filter_operator_{i}', 'AND').upper()
        #         comparison_operator = request.args.get(f'filter_op_{i}', '=').lower()

        #         if not column_name:
        #             continue

        #         if hasattr(model, column_name):
        #             column = getattr(model, column_name)

        #             try:
        #                 python_type = getattr(column.type, 'python_type', str)

        #                 # Handle special operators first
        #                 if comparison_operator in ['is null', 'isnone']:
        #                     condition = column.is_(None)
        #                 elif comparison_operator in ['is not null', 'isnotnone']:
        #                     condition = column.isnot(None)
        #                 elif comparison_operator in ['in', 'not in']:
        #                     if not filter_value:
        #                         continue
        #                     values = [v.strip() for v in filter_value.split(',')]
        #                     # Try to cast if numeric type
        #                     if issubclass(python_type, (int, float)):
        #                         try:
        #                             values = [python_type(v) for v in values]
        #                         except ValueError:
        #                             continue
        #                     condition = column.in_(values) if comparison_operator == 'in' else ~column.in_(values)
        #                 else:
        #                     # For standard comparison operators
        #                     if filter_value is None:
        #                         continue
        #                     if issubclass(python_type, (int, float)):
        #                         try:
        #                             filter_value = python_type(filter_value)
        #                         except ValueError:
        #                             continue
        #                     elif issubclass(python_type, str):
        #                         filter_value = str(filter_value)

        #                     if comparison_operator == '=':
        #                         condition = column == filter_value
        #                     elif comparison_operator == '!=':
        #                         condition = column != filter_value
        #                     elif comparison_operator == '>':
        #                         condition = column > filter_value
        #                     elif comparison_operator == '<':
        #                         condition = column < filter_value
        #                     elif comparison_operator == '>=':
        #                         condition = column >= filter_value
        #                     elif comparison_operator == '<=':
        #                         condition = column <= filter_value
        #                     elif comparison_operator == 'ilike' and issubclass(python_type, str):
        #                         condition = column.ilike(f'%{filter_value}%')
        #                     else:
        #                         print(f"DEBUG: Unsupported operator '{comparison_operator}' for column '{column_name}'")
        #                         continue

        #                 # Combine filters
        #                 if combined_filter is None:
        #                     combined_filter = condition
        #                 elif logical_operator == 'AND':
        #                     combined_filter = and_(combined_filter, condition)
        #                 elif logical_operator == 'OR':
        #                     combined_filter = or_(combined_filter, condition)

        #                 print(f"DEBUG: Added filter: {column_name} {comparison_operator.upper()} {filter_value} with {logical_operator}")
        #             except Exception as e:
        #                 print(f"DEBUG: Error applying filter on {column_name}: {str(e)}")
        #         else:
        #             print(f"DEBUG: Invalid filter column: {column_name}")
        
        combined_filter = self.get_filters_from_request(request, model)
        
        # Apply the combined filter if any  'SELECT eyedro.gb_0098063d.epoch_secs AS eyedro_gb_0098063d_epoch_secs, eyedro.gb_0098063d.ts AS eyedro_gb_0098063d_ts, eyedro.gb_0098063d.a_p1 AS eyedro_gb_0098063d_a_p1, eyedro.gb_0098063d.a_p2 AS eyedro_gb_0098063d_a_p2, eyedro.gb_0098063d.a_p3 AS eyedro_gb_0098063d_a_p3, eyedro.gb_0098063d.v_p1 AS eyedro_gb_0098063d_v_p1, eyedro.gb_0098063d.v_p2 AS eyedro_gb_0098063d_v_p2, eyedro.gb_0098063d.v_p3 AS eyedro_gb_0098063d_v_p3, eyedro.gb_0098063d.pf_p1 AS eyedro_gb_0098063d_pf_p1, eyedro.gb_0098063d.pf_p2 AS eyedro_gb_0098063d_pf_p2, eyedro.gb_0098063d.pf_p3 AS eyedro_gb_0098063d_pf_p3, eyedro.gb_0098063d.wh_p1 AS eyedro_gb_0098063d_wh_p1, eyedro.gb_0098063d.wh_p2 AS eyedro_gb_0098063d_wh_p2, eyedro.gb_0098063d.wh_p3 AS eyedro_gb_0098063d_wh_p3, eyedro.gb_0098063d.api_flag AS eyedro_gb_0098063d_api_flag \nFROM eyedro.gb_0098063d \nWHERE CAST(eyedro.gb_0098063d.ts AS DATE) BETWEEN %(param_1)s AND %(param_2)s AND eyedro.gb_0098063d.a_p1 < %(a_p1_1)s AND eyedro.gb_0098063d.a_p2 < %(a_p2_1)s'
        if combined_filter is not None:
            query = query.filter(combined_filter)
            #! sqlalchemy < v2
            #self.count_query = query.statement.with_only_columns([func.count()]).order_by(None)
            #! sqlalchemy >= v2
            self.count_query = query.statement.with_only_columns(func.count()).order_by(None)

            # Get the count with filters applied
            count = self.session.scalar(self.count_query)
        
        
        # Apply pagination at the database level
        page_size = page_size or self.page_size
        offset = (page - 1) * page_size if page else 0
        
        # Get primary key for consistent ordering
        if model.__table__.primary_key.columns:
            pk_column = next(iter(model.__table__.primary_key.columns))
            query = query.order_by(pk_column)
        
        # Get rows with pagination
        rows = query.limit(page_size).offset(offset).all()
        compiled = query.statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
        print(str(compiled))

        # Process the rows
        processed_rows = []
        for idx, row in enumerate(rows):
            # if row is None:
            #     continue
            row_data = {
                col.name: getattr(row, col.name)
                for col in model.__table__.columns
            }
            processed_rows.append(DynamicRowModel(row_data))
    
        # Print debug info
        print(f"DEBUG: Query returned {count} total rows")
        print(f"DEBUG: Returning {len(processed_rows)} rows for page {page}")
        print(f"DEBUG: Query SQL: {query.statement.compile(dialect=engine.dialect, compile_kwargs={'literal_binds': True})}")
        
        return count, processed_rows, query.statement.compile(dialect=engine.dialect, compile_kwargs={'literal_binds': True})


    def get_query(self, page, model):
        """Alternative approach to construct the query with filters."""
        # Dynamically determine the primary key column(s)
        primary_key_columns = model.__table__.primary_key.columns
        if not primary_key_columns:
            raise AttributeError(f"The model '{model.__name__}' has no primary key.")
        
        # Assume the primary key is a single column and use it for ordering.
        primary_key_column = next(iter(primary_key_columns)).name
        
        # Build filter conditions
        filter_conditions = []
        if has_request_context():
            i = 0
            while request.args.get(f'filter_column_{i}') is not None:
                column_name = request.args.get(f'filter_column_{i}')
                filter_value = request.args.get(f'filter_value_{i}')
                operator = request.args.get(f'filter_operator_{i}')
                
                print(f"DEBUG: Processing filter {i}: column={column_name}, value={filter_value}, operator={operator}")
                
                if column_name and filter_value and hasattr(model, column_name):
                    column = getattr(model, column_name)
                    
                    # Create condition based on column type
                    try:
                        if hasattr(column.type, 'python_type'):
                            column_type = column.type.python_type
                            
                            if issubclass(column_type, (int, float)):
                                try:
                                    filter_value = column_type(filter_value)
                                    condition = column == filter_value
                                except ValueError:
                                    i += 1
                                    continue
                            elif issubclass(column_type, str):
                                condition = column.ilike(f'%{filter_value}%')
                            else:
                                condition = column == filter_value
                        else:
                            condition = column.ilike(f'%{filter_value}%')
                        
                        # Combine conditions with AND or OR
                        if i == 0 or not filter_conditions:
                            filter_conditions.append(condition)
                        elif operator == 'OR':
                            filter_conditions.append(or_(filter_conditions[-1], condition))
                            filter_conditions.pop(0)  # Remove previous combined condition
                        else:  # Default to AND
                            filter_conditions.append(and_(filter_conditions[-1], condition))
                            filter_conditions.pop(0)  # Remove previous combined condition
                    except Exception as e:
                        print(f"DEBUG: Error creating filter: {str(e)}")
                
                i += 1
        
        # Create initial query
        query = self.session.query(model)
        
        # Apply filter conditions one by one
        if filter_conditions:
            query = query.filter(filter_conditions[-1])  # Apply the final combined condition
        
        # Add row_number
        row_number_column = func.row_number().over(order_by=getattr(model, primary_key_column)).label("row_number")
        
        # Create final query
        final_query = self.session.query(model, row_number_column).from_statement(
            query.statement
        )
        
        # Print the SQL
        sql = str(final_query.statement.compile(
            dialect=engine.dialect,
            compile_kwargs={"literal_binds": True}
        ))
        print(f"DEBUG: Generated SQL: {sql}")
        
        return final_query



    def get_count_query(self, model):
        """Construct a query to get the total row count."""
        return self.session.scalar(self.count_query) #self.session.query(func.count()).select_from(model)
        

    def get_pagination_data(self, page, row_count, page_size):
        total_pages = (row_count // page_size) + (1 if row_count % page_size else 0)
        is_large_dataset = row_count > 1_000_000

        # Limit to 10 pages in view
        page_window = 5
        start_page = max(1, page - page_window)
        end_page = min(total_pages, page + page_window)

        return {
            'page': page,
            'total_pages': total_pages,
            'has_prev': page > 1,
            'has_next': page < total_pages,
            'prev_page': page - 1 if page > 1 else None,
            'next_page': page + 1 if page < total_pages else None,
            'page_range': range(start_page, end_page + 1),
            'start_page': start_page,         # ✅ Added
            'end_page': end_page,             # ✅ Added
            'is_large_dataset': is_large_dataset,
            'page_size': self.page_size
        }
        
        
    def render(self, template, **kwargs):
        """Override render method to add pagination data."""
        model = self.get_model()
        if model:
            self.model = model
            self._refresh_cache()
            
           # Extract date/datetime columns
        date_columns = [
            col.name
            for col in model.__table__.columns
            if isinstance(col.type, (Date, DateTime)) or 'date' in col.name.lower()
        ]

        # Get pagination data and add it to the template context
        page = request.args.get('page', 1, type=int)
        row_count = self.get_count_query(model)
        pagination_data = self.get_pagination_data(page, row_count, self.page_size)
        
        jump = request.args.get("jump", type=int)
        page = request.args.get("page", 1, type=int)
        total_pages = (row_count // self.page_size) + (1 if row_count % self.page_size else 0)

        if jump:
            page += jump
            page = max(1, min(page, total_pages))

        # Add column list to the template context
        return super().render(
            template, 
            pagination=pagination_data, 
            column_list=self.column_list, 
            column_labels=self.column_labels, 
            date_columns=date_columns, 
            **kwargs)


    def _refresh_cache(self):
        """Refresh the cache to update model and column data."""
        model = self.get_model()
        if model:
            self.model = model
            # self.column_list = ['row_number'] + [col.name for col in model.__table__.columns]
            # self.column_labels = {'row_number': '#'}
            # self.column_labels.update({
            #     col.name: col.name.capitalize() for col in model.__table__.columns
            # })
            self.column_list =  [col.name for col in model.__table__.columns]
            self.column_labels = {col.name: col.name.capitalize() for col in model.__table__.columns}

        super()._refresh_cache()

    def is_accessible(self):
        """Check if the view is accessible."""
        model_exists = self.get_model() is not None
        self._refresh_cache()
        return model_exists

    def get_foreign_key_columns(self):
        fk_info = {}
        for col in self.model.__table__.columns:
            for fk in col.foreign_keys:
                fk_info[col.name] = {
                    'referred_table': fk.column.table.name,
                    'referred_column': fk.column.name
                }
        return fk_info

    def scaffold_form(self):
        """Scaffold the form dynamically based on the model's columns."""
        if self._is_initializing:
            return super().scaffold_form()

        model = self.get_model()
        if not model:
            return super().scaffold_form()

        self.model = model

        # Extract columns involved in the primary key (composite PK)
        pk_columns = [col.name for col in model.__table__.primary_key.columns]

        # Get foreign key columns
        fk_columns = [
            col.name for col in self.model.__table__.columns if col.foreign_keys
        ]

        # Build list of editable columns (exclude PK and FK)
        form_columns = [
            col.name
            for col in model.__table__.columns
            if col.name not in pk_columns  # exclude primary keys
        ]
        self.form_columns = form_columns

        class DynamicModelForm(FlaskForm):
            pass

        # Loop through all columns to build fields
        for col in model.__table__.columns:
            col_name = col.name

            # Mark pk or fk as read-only
            is_readonly = col_name in pk_columns or col_name in fk_columns

            field = None
            kwargs = {'validators': [DataRequired()]} if not is_readonly else {}
            
            # If the field is read-only, apply the 'ReadOnlyInput' widget
            if is_readonly:
                kwargs['render_kw'] = {'readonly': True}
                kwargs['widget'] = ReadOnlyInput()  # Apply the custom widget

            # Handle different column types and create fields accordingly
            if isinstance(col.type, Integer):
                field = IntegerField(col_name.capitalize(), **kwargs)
            elif isinstance(col.type, String):
                field = StringField(col_name.capitalize(), **kwargs)
            elif isinstance(col.type, Boolean):
                field = BooleanField(col_name.capitalize())
            elif isinstance(col.type, Date):
                field = DateField(col_name.capitalize(), **kwargs)

            if field:
                setattr(DynamicModelForm, col_name, field)

        return DynamicModelForm

        def get_pk_value(self, model):
            """Get the primary key value for a given model instance."""
            if hasattr(model, 'row_number'):
                return getattr(model, 'row_number')
            if hasattr(model, '__table__'):
                pk_columns = [col.name for col in model.__table__.primary_key.columns]
                if len(pk_columns) == 1:
                    return getattr(model, pk_columns[0])
                elif pk_columns:
                    return tuple(getattr(model, col) for col in pk_columns)
            return None

        def handle_action(self, return_view=None):
            """Handle actions like delete for the selected rows."""
            model = self.get_model()
            if not model:
                return redirect(self.get_url(".index_view"))

            selected_rows = request.form.getlist("rowid")
            if selected_rows:
                pk_columns = [col.name for col in model.__table__.primary_key.columns]  # ✅ fixed line
                for pk_str in selected_rows:
                    try:
                        if len(pk_columns) > 1:
                            pk_values = eval(pk_str)
                            filters = {col: val for col, val in zip(pk_columns, pk_values)}
                            row = self.session.query(model).filter_by(**filters).first()
                        else:
                            row = self.session.get(model, pk_str)
                        if row:
                            self.session.delete(row)
                    except Exception as e:
                        flash(f"Error deleting row: {str(e)}", "error")

                try:
                    self.session.commit()
                    flash(f"Deleted {len(selected_rows)} row(s).", "success")
                except Exception as e:
                    self.session.rollback()
                    flash(f"Commit failed: {str(e)}", "error")

            return redirect(self.get_url(".index_view"))

    def inaccessible_callback(self, name, **kwargs):
        """Handle case when view is inaccessible."""
        print("DEBUG: Session schema =", flask_session.get("schema"))
        print("DEBUG: Session table =", flask_session.get("table"))
        flash("Please select a schema and table first.")
        #return redirect("/")
        return redirect("/admin/dynamictable")


admin = Admin(app, name="UNHCR AZURE Admin", template_mode="bootstrap4")

# Register the DynamicTableView with the admin interface
admin.add_view(
    DynamicTableView(db.session, name="DynamicTable", endpoint="dynamictable")
)


_query_cache = {}
CACHE_TTL_SECONDS = 60 * 30  # 30 minutes

def get_tables_with_counts(schema_name):
    now = time.time()

    # Check cache
    cache_entry = _query_cache.get(schema_name)
    if cache_entry:
        cached_time, cached_data = cache_entry
        if now - cached_time < CACHE_TTL_SECONDS:
            return cached_data

    query = text("""
        SELECT
            h.hypertable_name AS table_name,
            SUM(c.reltuples)::BIGINT AS estimated_rows,
            pg_size_pretty(SUM(pg_total_relation_size(quote_ident(h.chunk_schema) || '.' || quote_ident(h.chunk_name)))) AS total_size
        FROM
            timescaledb_information.chunks h
            JOIN pg_class c ON c.relname = h.chunk_name
            JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE
            h.hypertable_schema = :schema
        GROUP BY h.hypertable_name
        ORDER BY estimated_rows DESC;
    """)

    result = db.session.execute(query, {"schema": schema_name})
    res = [(row.table_name, row.estimated_rows, row.total_size) for row in result]

    query = text("""
        SELECT
        c.relname AS table_name,
        c.reltuples::BIGINT AS estimated_rows,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
        FROM
        pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE
        n.nspname = :schema
        AND c.relkind = 'r'  -- 'r' = regular table
        ORDER BY
        pg_total_relation_size(c.oid) DESC;
    """)

    # Build a quick lookup: table_name -> (estimated_rows, total_size)
    res_dict = {item[0]: (item[1], item[2]) for item in res}

    result = db.session.execute(query, {"schema": schema_name})

    for row in result:
        existing = res_dict.get(row.table_name)

        # If not exists OR new row has more estimated rows, update
        if not existing or row.estimated_rows > existing[0]:
            res_dict[row.table_name] = (row.estimated_rows, row.total_size)

    # Convert back to list of tuples if needed
    res = [(table, est_rows, size) for table, (est_rows, size) in res_dict.items()]
    _query_cache[schema_name] = (now, res)
    return res


@app.before_request
def require_login():
    public_endpoints = ['login','logout', 'alive', 'static']
    
    if request.endpoint in public_endpoints:
        return

    if not flask_session.get('logged_in'):
        flash(f"Please log in to access this page: {format(request.url).split('?')[0]}")
        return redirect(url_for('login'))


@app.route("/dynamictable")
def dynamictable_view():
    return redirect("/admin/dynamictable")


@app.route("/", methods=["GET", "POST"])
def index():
    #flask_session.pop("schema", None)
    #flask_session.pop("table", None)
    schemas = sorted([s for s in inspector.get_schema_names() if s in ALLOWED_SCHEMAS])
    default_schema = "solarman"
    selected_schema = request.form.get("schema")
    selected_table = request.form.get("tabledb")
    prev_schema = request.form.get("prev_schema")
    if selected_schema is None and 'schema' in flask_session:
        selected_schema = flask_session['schema']
    if selected_table is None and 'table' in flask_session:
        selected_table = flask_session['table']
    if selected_schema is None:
        selected_schema = default_schema

    tables = []

    if request.method == "POST":
        if prev_schema != selected_schema:
            selected_table = None
            tables = sorted(get_tables_with_counts(selected_schema))

        if (
            selected_schema
            and selected_table
            and selected_schema != "-- choose schema --"
            and selected_table != "-- choose table --"
        ):
            flask_session["schema"] = selected_schema
            flask_session["table"] = selected_table
            return redirect("/admin/dynamictable")

    if selected_schema and selected_schema != "-- choose schema --":
            tables = sorted(get_tables_with_counts(selected_schema))

    return render_template(
        "index.html",
        schemas=schemas,
        tables=tables,
        selected_schema= selected_schema,
        selected_table= selected_table,
        logs=[],
    )

#cd /home/unhcr_admin/code/unhcr_module/web_app && pgrep -fx "sudo /bin/bash ./scripts/web_app.sh" > /dev/null || (sudo /bin/bash ./scripts/web_app.sh | sudo tee -a /datadrive/logs/web_app.log 2>&1 && sudo bash -c 'echo $(( $(cat /datadrive/logs/run_count_web_app.log 2>/dev/null || echo 0) + 1 )) > /datadrive/logs/run_count_web_app.log')
@app.route("/alive", methods=["GET"])
def alive():
    return 'ALIVE', 200

@app.route("/admin")
def go_admin():
    #flask_session.pop("schema", None)
    #flask_session.pop("table", None)
    return redirect("/")

admin_rt = '/admin/dynamictable'
@app.route(admin_rt, methods=["GET", "POST"])
def index_view():
    # Print all request arguments for debugging
    print("DEBUG: Request args:", request.args)
    
    page = request.args.get('page', 1, type=int)

    # Create the dynamic admin view
    table_view = DynamicTableView(session=db.session)
    table_view.admin = admin
    
    # Get the model based on schema and table
    model = table_view.get_model()
    if not model:
        flash("Please select a schema and table first.")
        return redirect("/")

    # Handle form submission (editing rows)
    if request.method == "POST":
        formdata = request.form.to_dict()
        print("DEBUG: Form submission:", formdata)

        # Get the original primary key values
        pk_filters = []
        for pk_col in model.__table__.primary_key.columns:
            orig_value = formdata.get(f"orig_{pk_col.name}")
            if orig_value is None:
                flash(f"Missing original value for PK column: {pk_col.name}")
                return redirect('/')

            try:
                # Cast to correct type
                python_type = pk_col.type.python_type
                if orig_value in ( "None", None):
                    typed_value = None
                elif python_type in (datetime, date):
                    typed_value = table_view.parse_date(orig_value)
                else:
                    typed_value = python_type(orig_value)
            except Exception as e:
                flash(f"Failed to cast {orig_value} for {pk_col.name}: {e}")
                return redirect(request.url)

            pk_filters.append(getattr(model, pk_col.name) == typed_value)

        obj = db.session.query(model).filter(*pk_filters).first()
        if obj:
            # Update attributes
            # Columns to exclude from update
            excluded_fields = {"created", "updated"}

            for field, value in formdata.items():
                if field.startswith("orig_pk_") or field in excluded_fields:
                    continue

                if not hasattr(obj, field):
                    continue

                col = getattr(model, field, None)
                column_obj = model.__table__.columns.get(field)

                if column_obj is None:
                    continue

                python_type = column_obj.type.python_type

                if value in ("", "None", None):
                    if not isinstance(column_obj.type, String):  # Empty string is valid for strings
                        if not column_obj.nullable:
                            flash(f"{field} is required and cannot be empty.")
                            return redirect(request.url)
                        value = None
                else:
                    try:
                        if python_type in (datetime, date):
                            value = table_view.parse_date(value)
                        else:
                            value = python_type(value)
                    except Exception as e:
                        flash(f"Error casting {field}: {e}")
                        return redirect(request.url)

                setattr(obj, field, value)

            db.session.commit()
            flash("Row updated.")
        else:
            flash("Row not found.")
        return redirect(request.url)
    
    # Refresh column metadata
    table_view._refresh_cache()

    # Extract date-like columns
    date_columns = [
        col.name for col in model.__table__.columns
        if isinstance(col.type, (Date, DateTime)) or 'date' in col.name.lower() or col.name.lower() in ('created', 'updated')
    ]
    
    col_types =  {col.name: str(col.type) for col in model.__table__.columns}

    # Filter handling
    filter_params = {}
    i = 0
    while request.args.get(f'filter_column_{i}') is not None:
        filter_params[f'filter_column_{i}'] = request.args.get(f'filter_column_{i}')
        filter_params[f'filter_value_{i}'] = request.args.get(f'filter_value_{i}')
        if i > 0:
            filter_params[f'filter_operator_{i}'] = request.args.get(f'filter_operator_{i}', 'AND')
        i += 1

    print("DEBUG: Filter params:", filter_params)
    
    # Call get_list to get count and rows
    count, rows, sql = table_view.get_list(page=page, sort_column=None, sort_desc=None, search=None, filters=None)
    
    # Generate pagination data
    pagination_data = table_view.get_pagination_data(page, count, table_view.page_size)

    # Pass the data to the template
    return table_view.render(
        'admin/custom_list.html', 
        data=rows, 
        count=count, 
        foreign_key_info=table_view.get_foreign_key_columns(),
        primary_key_columns=table_view.primary_key_columns,
        filter_params=filter_params,
        pagination_data=pagination_data,
        col_types=col_types,
        current_epoch=int(time.time()),
        date_column=request.args.get('date_column')
    )


@app.route('/add_record', methods=['POST'])
def add_record():
    try:
        schema = flask_session.get("schema")
        table = flask_session.get("table")
        if not schema or not table:
            flash('Schema and table are required.', 'error')
            return redirect('admin/dynamictable')

        table_view = DynamicTableView(session=db.session)
        table_view.admin = admin
        
        # Get the model based on schema and table
        model = table_view.get_model()
        if not model:
            flash("Unable to get model.")
            return redirect("/")

        # Get all form data except schema and table
        record_data = {key: value for key, value in request.form.items() if key not in ['schema_name', 'table_name']}

        # Loop through the columns and apply dynamic type conversion based on the column types
        for key, value in record_data.items():
            # Get the column type from the model
            column_type = model.__table__.columns[key].type
            
            # Convert value based on the column type
            if isinstance(column_type, (Integer, BigInteger)):  # Handle both Integer and BigInteger
                # Ensure the value is a valid integer
                try:
                    record_data[key] = int(value)
                except ValueError:
                    record_data[key] = None  # Or handle the invalid integer case accordingly
            elif isinstance(column_type, String):
                record_data[key] = str(value)
            elif isinstance(column_type, DateTime):
                # Convert to datetime (assuming it's an epoch time)
                try:
                    record_data[key] = datetime.fromtimestamp(int(value))  # Assuming it's an epoch time
                except ValueError:
                    record_data[key] = None  # Handle invalid datetime
            elif isinstance(column_type, Float):
                try:
                    record_data[key] = float(value)
                except ValueError:
                    record_data[key] = None  # Handle invalid float
            # Add other types as needed (e.g., Date, Boolean, etc.)

        # Construct the INSERT query
        columns = ', '.join(record_data.keys())
        placeholders = ', '.join([f":{key}" for key in record_data])
        query = text(f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})")

        # Execute the query with sanitized inputs
        db.session.execute(query, record_data)
        db.session.commit()
        
        flash('Record added successfully.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error adding record: {str(e)}', 'error')
    
    return redirect('admin/dynamictable')


@app.route('/delete_record', methods=['POST'])
def delete_record():
    try:
        schema = flask_session.get("schema")
        table = flask_session.get("table")
        if not schema or not table:
            flash('Schema and table are required.', 'error')
            return redirect('admin/dynamictable')

        table_view = DynamicTableView(session=db.session)
        table_view.admin = admin
        
        # Get the model based on schema and table
        model = table_view.get_model()
        if not model:
            flash("Unable to get model.")
            return redirect("/")

        # Handle form submission (delete row)
        formdata = request.form.to_dict()
        print("DEBUG: Form submission:", formdata)
        primary_keys = {}
        for pk_col in model.__table__.primary_key.columns:
            orig_value = formdata.get(f"orig_{pk_col.name}")
            if orig_value is None:
                flash(f"Missing original value for PK column: {pk_col.name}")
                return redirect('/')
            primary_keys[pk_col.name] = orig_value  # ✅ Build the dict

        # Build WHERE clause dynamically
        where_clause = " AND ".join([f"{col} = :{col}" for col in primary_keys])
        sql = text(f"DELETE FROM {schema}.{table} WHERE {where_clause}")

        db.session.execute(sql, primary_keys)
        db.session.commit()

        flash('Record deleted successfully.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting record: {str(e)}', 'error')
    
    return redirect('admin/dynamictable')  # Or return JSON if it's AJAX


@app.route("/download", methods=["GET", "POST"])
def download():
    try:
        page = request.args.get('page', 1, type=int)
        schema = flask_session['schema']
        table = flask_session['table']
        # Create the dynamic admin view
        table_view = DynamicTableView(session=db.session)
        table_view.admin = admin
        
        # Get the model based on schema and table
        model = table_view.get_model()
        if not model:
            flash("Please select a schema and table first.")
            return redirect("/")
        
        count, rows, sql = table_view.get_list(page=page, sort_column=None, sort_desc=None, search=None, filters=None)
        result_proxy = db.session.execute(text(str(sql)))
        columns = result_proxy.keys()  # column headers

        def generate_csv():
            yield ",".join(columns) + "\n"
            for row in result_proxy:  # stream row by row
                yield ",".join(str(item) if item is not None else "" for item in row) + "\n"

        filename = f"{schema}_{table}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        response = Response(
            response=generate_csv(),
            headers={
                "Content-Type": "text/csv",
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

        return response

    except Exception as e:
        flash(f"Error during download: {str(e)}", "error")
        return redirect(request.referrer or "/")


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password')

        if email.endswith('@unhcr.org') and password == PASSWORD:
            flask_session['logged_in'] = True
            flask_session['user_email'] = email
            return redirect('/')
        else:
            error = "Invalid email or password"

    return render_template('login.html', error=error)


@app.route("/logout", methods=["POST"])
def logout():
    flask_session.pop("logged_in", None)
    return redirect(url_for("login"))

if __name__ == "__main__":
     app.run(host='0.0.0.0', port=5000, debug=True)
