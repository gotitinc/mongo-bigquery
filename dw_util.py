import json
import abc
import re
from sets import Set
import pprint


class DataWarehouse:
  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def create_dataset(self, database_name):
    return

  @abc.abstractmethod
  def delete_dataset(self, database_name):
    return

  @abc.abstractmethod
  def create_table(self, database_name, table_name, schema_file_name, process_array):
    return

  @abc.abstractmethod
  def update_table(self, database_name, table_name, schema_file_name):
    return

  @abc.abstractmethod
  def delete_table(self, database_name, table_name):
    return

  @abc.abstractmethod
  def get_num_rows(self, database_name, table_name):
    return

  @abc.abstractmethod
  def table_exists(self, database_name, table_name):
    return

  @abc.abstractmethod
  def get_table_schema(self, database_name, table_name):
    return

  @abc.abstractmethod
  def get_job_state(self, job_id):
    return

  @abc.abstractmethod
  def list_tables(self, database_name, table_prefix):
    return

  @abc.abstractmethod
  def load_table(self, table_name, file_path):
    return

  @abc.abstractmethod
  def query(self, query):
    return


class Redshift(DataWarehouse):

  connection_string = None

  def __init__(self, connection_string):
    print '-- Initializing Redshift Util --'
    self.connection_string = connection_string

  def execute_sql (self, sql, fetch_result = False):
    # create connection
    import psycopg2
    conn = psycopg2.connect(self.connection_string)
    cur = conn.cursor()

    # execute
    print "Executing SQL: %s" % (sql)
    cur.execute(sql)

    output = []
    if fetch_result:
      rows = cur.fetchall()
      for row in rows:
        output.append(row)

    cur.close()

    # commit
    conn.commit()
    conn.close()

    return output

  def create_dataset(self, database_name):
    pass

  def delete_dataset(self, database_name):
    pass

  def flatten(self, schema, fields, parent=None):
    for node in schema:
      if node["mode"].lower() == "repeated":
        if parent == None:
          if "fields" in node:
            self.flatten(node["fields"], fields, node["name"])
            del node['fields']
        else:
          if "fields" in node:
            self.flatten(node["fields"], fields, parent + "." + node["name"])
            del node['fields']

      if parent != None:
        node["name"] = parent + "." + node["name"]

      fields.append(node)

  def create_table(self, database_name, table_name, schema_file_name, process_array):

    # load schema from file
    schema = json.load(open(schema_file_name))

    # flatten
    fields = []
    self.flatten(schema, fields, None)

    # used to keep track of table_name -> column list
    table_columns = {}

    for field in fields:
      data_type = None

      if field['type'] == 'string':
        data_type = 'varchar'
      elif field['type'] in ('float', 'timestamp', 'boolean'):
        data_type = field['type']
      elif field['type'] ==  'integer':
        data_type = 'BIGINT'
      elif field['type'] in ('record'):
        # ignore record
        pass
      else:
        raise Exception("Unsupported data type %s for column %s" % (field['type'], field['name']))

      if data_type is not None:
        if field['mode'] == 'repeated':
          table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name']).lower()
          column_name = "value"
        else:
          if "." in field['name']:
            table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name'].rsplit(".",1)[0]).lower()
            column_name = field['name'].rsplit(".",1)[1]
          else:
            table_name = table_name
            column_name = field['name']

        if table_name not in table_columns:
          table_columns[table_name] = []
          if table_name != table_name:
            table_columns[table_name].append("%s %s" % ("parent_hash_code", "varchar"))
            table_columns[table_name].append("%s %s" % ("hash_code", "varchar"))

        table_columns[table_name].append("%s %s" % (column_name, data_type))

    for table_name, columns in table_columns.iteritems():
      sql = "create table %s (%s)" % (table_name, ",".join(columns))
      self.execute_sql(sql)

  def update_table(self, database_name, table_name, schema_file_name):

    # load schema from file
    schema = json.load(open(schema_file_name))

    # flatten
    fields = []
    self.flatten(schema, fields, None)

    # current columns
    table_names = self.list_tables("", "", table_name)
    current_table_columns = {}
    for table_name in table_names:
      current_columns = {}
      current_schema = self.get_table_schema(database_name, table_name)
      for field in current_schema:
        current_columns[field['name']] = field['type']
      current_table_columns[table_name] = current_columns

    # used to keep track of table_name -> column list
    new_table_columns = {}

    alter_sqls = []
    modify_instructions = {}

    for field in fields:

      # print "processing field %s" % str(field)
      sql_data_type = None

      if field['type'] == 'string':
        sql_data_type = 'varchar'
      elif field['type'] in ('float', 'timestamp', 'boolean'):
        sql_data_type = field['type']
      elif field['type'] ==  'integer':
        sql_data_type = 'BIGINT'
      elif field['type'] in ('record'):
        # ignore record
        pass
      else:
        raise Exception("Unsupported data type %s for column %s" % (field['type'], field['name']))

      if sql_data_type is not None:

        if field['mode'] == 'repeated':
          table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name']).lower()
          column_name = "value"
        else:
          if "." in field['name']:
            table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name'].rsplit(".",1)[0]).lower()
            column_name = field['name'].rsplit(".",1)[1]
          else:
            table_name = table_name
            column_name = field['name']

        # print "column name %s" % column_name
        if table_name in current_table_columns:
          current_columns = current_table_columns[table_name]
          if column_name in current_columns:
            # print "  column %s found in current table schema." % column_name
            if field['type'].lower() != current_columns[column_name].lower():
              # print "  but data type is different. new: %s old: %s" % (field['type'], current_columns[column_name])
              if table_name not in modify_instructions:
                modify_instructions[table_name] = {}
              modify_instructions[table_name][column_name] = sql_data_type
            else:
              # print "  data type is same.. no-op."
              pass
          else:
            # print "  column %s not found in current table schema." % column_name
            alter_sqls.append ("alter table %s add column %s %s" % (table_name, column_name, sql_data_type))

        else:
          # new table needed
          if table_name not in new_table_columns:
            new_table_columns[table_name] = []
            new_table_columns[table_name].append("%s %s" % ("parent_hash_code", "varchar"))
            new_table_columns[table_name].append("%s %s" % ("hash_code", "varchar"))
          new_table_columns[table_name].append("%s %s" % (column_name, sql_data_type))

    # generate sqls to modify column data type
    modify_sqls = []
    for table_name, modify_columns in modify_instructions.iteritems():

      select_items = []
      current_columns = current_table_columns[table_name]
      for current_column in current_columns:

        if current_column in modify_columns:
          select_items.append("cast(%s as %s) as %s" % (current_column, modify_columns[current_column], current_column))
        else:
          select_items.append(current_column)

      tmp_table_name = table_name + "_update_schema"
      modify_sqls.append("drop table if exists %s" % (tmp_table_name))
      modify_sqls.append("alter table %s rename to %s" % (table_name, tmp_table_name))
      modify_sqls.append("create table %s as select %s from %s" % (table_name, ", ".join(select_items), tmp_table_name))

    for sql in modify_sqls:
      self.execute_sql(sql)

    for sql in alter_sqls:
      self.execute_sql(sql)

    for table_name, columns in new_table_columns.iteritems():
      sql = "create table %s (%s)" % (table_name, ",".join(columns))
      self.execute_sql(sql)

    return {}


  def delete_table(self, database_name, table_name):
    sql = "drop table if exists %s" % (table_name)
    self.execute_sql(sql, False)

    child_table_names = self.list_tables(database_name, table_name)
    for child_table_name in child_table_names:
      sql = "drop table if exists %s" % (child_table_name)
      self.execute_sql(sql, False)

  def get_num_rows(self, database_name, table_name):
    sql = "select count(*) from %s" % (table_name)
    r = self.execute_sql(sql, True)
    return r[0][0]

  def table_exists(self, database_name, table_name):
    sql = "select count(*) from pg_table_def where tablename = '%s' and schemaname = 'public'" % (table_name)
    r = self.execute_sql(sql, True)
    if r[0][0] > 0:
      return True
    return False

  def get_table_schema(self, database_name, table_name):

    sql = "select \"column\", type from pg_table_def where tablename = '%s' and schemaname = 'public'" % (table_name)
    r = self.execute_sql(sql, True)

    fields = []
    for row in r:
      d = {}
      if 'character' in row[1]:
        d['type'] = 'STRING'
      elif 'double' in row[1]:
        d['type'] = 'FLOAT'
      elif 'integer' in row[1] or 'bigint' in row[1]:
        d['type'] = 'INTEGER'
      elif 'timestamp' in row[1]:
        d['type'] = 'TIMESTAMP'
      elif 'boolean' in row[1]:
        d['type'] = 'BOOLEAN'

      d['name'] = row[0]
      d['mode'] = 'NULLABLE'
      fields.append(d)

    return fields

  def get_job_state(self, job_id):
    sql = "select min(status), min(errors), sum(lines_scanned) from (select min(status) as status , min(errors) as errors, min(lines_scanned) as lines_scanned from stl_load_commits where filename like '%%%s%%' group by filename) as a" % job_id
    r = self.execute_sql(sql, True)

    job_state = None
    job_result = None
    job_error_message = None
    job_error_reason = None
    job_output_rows = 0

    if len(r) > 0:
      row = r[0]
      status = row[0]
      if status == 1:
        job_state = "DONE"
      else:
        job_state = str(status)

      errors = row[1]
      if errors == -1:
        job_error_message = None
        job_error_reason = None
      else:
        job_error_message = str(errors)
        job_error_reason = str(errors)

      lines_scanned = row[2]
      job_output_rows = lines_scanned - 1

    return (job_state, job_result, job_error_message, job_error_reason, job_output_rows)


  def list_tables(self, database_name, table_prefix):
    sql = "select distinct table_name from information_schema.columns where table_schema = 'public' and table_name like '%s%%'" % table_prefix
    r = self.execute_sql(sql, True)
    output = []
    for row in r:
      output.append(row[0])
    return output

  def query(self, query):
    result = self.execute_sql(query, True)
    output = {}
    output['rows'] = []
    for r in result:
      f = []
      for i in r:
        f.append({"v": i})
      output['rows'].append({"f": f})

    return output


class Hive(DataWarehouse):

  host = None
  port = None
  hive_serdes_path = None

  def __init__(self, host, port, hive_serdes_path):
    print '-- Initializing Hive Util --'
    self.host = host
    self.port = port
    self.hive_serdes_path = hive_serdes_path

  def execute_sql (self, database_name, sql, fetch_result = False):
    import pyhs2
    conn = pyhs2.connect(host=self.host, port=self.port, authMechanism="NOSASL", database='default')

    # turn on tez and add serde jar
    c = conn.cursor()
    c.execute("set hive.execution.engine=tez")
    c.execute("set hive.cache.expr.evaluation=false")
    c.execute("add jar %s" % self.hive_serdes_path)
    c.execute("use %s" % database_name)

    # run actual command command
    print "Executing HiveQL: %s" % (sql)
    c.execute(sql)

    output = []
    if fetch_result:
      rows = c.fetchall()
      for row in rows:
        output.append(row)

    c.close()
    conn.close()

    return output

  def create_dataset(self, database_name):
    pass

  def delete_dataset(self, database_name):
    pass

  def flatten(self, schema, fields, parent=None):
    for node in schema:
      if node["mode"].lower() == "repeated":
        if parent == None:
          if "fields" in node:
            self.flatten(node["fields"], fields, node["name"])
            del node['fields']
        else:
          if "fields" in node:
            self.flatten(node["fields"], fields, parent + "." + node["name"])
            del node['fields']

      if parent != None:
        node["name"] = parent + "." + node["name"]

      fields.append(node)

  def create_table(self, database_name, table_name, schema_file_name, process_array = "child_table"):

    # load schema from file
    schema = json.load(open(schema_file_name))

    # flatten
    fields = []
    self.flatten(schema, fields, None)

    # used to keep track of table_name -> column list
    table_columns = {}

    for field in fields:
      data_type = None

      if field['type'] == 'string':
        data_type = 'string'
      elif field['type'] in ('timestamp', 'boolean'):
        data_type = field['type']
      elif field['type'] == 'float':
        data_type = 'double'
      elif field['type'] ==  'integer':
        data_type = 'int'
      elif field['type'] in ('record'):
        # ignore record
        pass
      else:
        raise Exception("Unsupported data type %s for column %s" % (field['type'], field['name']))

      if data_type is not None:
        if field['mode'] == 'repeated':
          if process_array == "child_table":
            child_table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name']).lower()
            column_name = "value"
          else:
            continue
        else:
          if "." in field['name']:
            if process_array == "child_table":
              child_table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name'].rsplit(".",1)[0]).lower()
              column_name = field['name'].rsplit(".",1)[1]
              print "  Child Table column:" + column_name
            else:
              child_table_name = table_name
              column_name = field['name'].split(".",1)[0]
              data_type = "string"
              print "  Inline column:" + column_name
          else:
            child_table_name = table_name
            column_name = field['name']

        if child_table_name not in table_columns:
          table_columns[child_table_name] = Set()
          if child_table_name != table_name:
            table_columns[child_table_name].add("%s %s" % ("parent_hash_code", "string"))
            table_columns[child_table_name].add("%s %s" % ("hash_code", "string"))

        table_columns[child_table_name].add("%s %s" % (column_name, data_type))

    for table_name, columns in table_columns.iteritems():
      sql = "create table %s (%s) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe' " % (table_name, ",".join(columns))
      self.execute_sql(database_name, sql)

  def update_table(self, database_name, table_name, schema_file_name):

    # load schema from file
    schema = json.load(open(schema_file_name))

    # flatten
    fields = []
    self.flatten(schema, fields, None)

    # current columns
    table_names = self.list_tables(database_name, table_name)
    current_table_columns = {}
    for table_name in table_names:
      current_columns = {}
      current_schema = self.get_table_schema(database_name, table_name)
      for field in current_schema:
        current_columns[field['name']] = field['type']
      current_table_columns[table_name] = current_columns

    # used to keep track of table_name -> column list
    new_table_columns = {}

    alter_sqls = []
    modify_instructions = {}

    for field in fields:

      # print "processing field %s" % str(field)
      sql_data_type = None

      if field['type'] == 'string':
        sql_data_type = 'string'
      elif field['type'] in ('timestamp', 'boolean'):
        sql_data_type = field['type']
      elif field['type'] == 'float':
        sql_data_type = 'double'
      elif field['type'] ==  'integer':
        sql_data_type = 'int'
      elif field['type'] in ('record'):
        # ignore record
        pass
      else:
        raise Exception("Unsupported data type %s for column %s" % (field['type'], field['name']))

      if sql_data_type is not None:

        if field['mode'] == 'repeated':
          child_table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name']).lower()
          column_name = "value"
        else:
          if "." in field['name']:
            child_table_name = table_name + "_" + re.sub("[^0-9a-zA-Z_]", '_', field['name'].rsplit(".",1)[0]).lower()
            column_name = field['name'].rsplit(".",1)[1]
          else:
            child_table_name = table_name
            column_name = field['name']

        # print "column name %s" % column_name
        if child_table_name in current_table_columns:
          current_columns = current_table_columns[child_table_name]
          if column_name in current_columns:
            print "  column %s found in current table schema." % column_name
            if field['type'].lower() != current_columns[column_name].lower():
              print "  but data type is different. new: %s old: %s" % (field['type'], current_columns[column_name])
              if child_table_name not in modify_instructions:
                modify_instructions[child_table_name] = {}
              modify_instructions[child_table_name][column_name] = sql_data_type
            else:
              print "  data type is same.. no-op."
              pass
          else:
            print "  column %s not found in current table schema." % column_name
            alter_sqls.append ("alter table %s add columns (%s %s)" % (child_table_name, column_name, sql_data_type))

        else:
          # new table needed
          if child_table_name not in new_table_columns:
            new_table_columns[child_table_name] = []
            new_table_columns[child_table_name].append("%s %s" % ("parent_hash_code", "string"))
            new_table_columns[child_table_name].append("%s %s" % ("hash_code", "string"))
          new_table_columns[child_table_name].append("%s %s" % (column_name, sql_data_type))

    # generate sqls to modify column data type
    modify_sqls = []
    for child_table_name, modify_columns in modify_instructions.iteritems():

      for modify_column_name, data_type in modify_columns.iteritems():
        modify_sqls.append("alter table %s change %s %s %s" % (child_table_name, modify_column_name, modify_column_name, data_type))

    for sql in modify_sqls:
      self.execute_sql(database_name, sql)

    for sql in alter_sqls:
      self.execute_sql(database_name, sql)

    for child_table_name, columns in new_table_columns.iteritems():
      sql = "create table %s (%s) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe' " % (child_table_name, ",".join(columns))
      self.execute_sql(database_name, sql)

    return {}

  def delete_table(self, database_name, table_name):
    sql = "drop table if exists %s" % (table_name)
    self.execute_sql(database_name, sql, False)

    child_table_names = self.list_tables(database_name, table_name)
    for child_table_name in child_table_names:
      sql = "drop table if exists %s" % (child_table_name)
      self.execute_sql(database_name, sql, False)

  def get_num_rows(self, database_name, table_name):
    sql = "select count(*) from %s" % (table_name)
    r = self.execute_sql(database_name, sql, True)
    return r[0][0]

  def table_exists(self, database_name, table_name):
    r = self.execute_sql(database_name, "show tables", True)
    for row in r:
      if row[0] == table_name:
        return True

    return False

  def get_table_schema(self, database_name, table_name):

    sql = "desc %s" % (table_name)
    r = self.execute_sql(database_name, sql, True)

    fields = []
    for row in r:
      d = {}
      if 'string' in row[1]:
        d['type'] = 'STRING'
      elif 'float' in row[1] or 'double' in row[1]:
        d['type'] = 'FLOAT'
      elif 'int' in row[1] or 'bigint' in row[1]:
        d['type'] = 'INTEGER'
      elif 'timestamp' in row[1]:
        d['type'] = 'TIMESTAMP'
      elif 'boolean' in row[1]:
        d['type'] = 'BOOLEAN'

      d['name'] = row[0]
      d['mode'] = 'NULLABLE'
      fields.append(d)

    return fields

  def get_job_state(self, job_id):

    job_state = None
    job_result = None
    job_error_message = None
    job_error_reason = None
    job_output_rows = 0

    return (job_state, job_result, job_error_message, job_error_reason, job_output_rows)


  def list_tables(self, database_name, table_prefix):
    sql = "show tables"
    r = self.execute_sql(database_name, sql, True)
    output = []
    for row in r:
      if row[0].startswith(table_prefix):
        output.append(row[0])
    return output

  def load_table(self, database_name, table_name, file_path):
    sql = "load data inpath '%s*' into table %s" % (file_path, table_name)
    self.execute_sql(database_name, sql, fetch_result = False)

  def query(self, database_name, query):
    result = self.execute_sql(database_name, query, True)
    output = {}
    output['rows'] = []
    for r in result:
      f = []
      for i in r:
        f.append({"v": i})
      output['rows'].append({"f": f})

    return output
