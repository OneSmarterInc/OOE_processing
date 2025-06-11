class DatabaseRouter:
    mssql_tables = ['Employee', 'Dependent','Eligibility','eligibility_status_table','custodial_data_table','history_data_table','empyp','depnp','elghp','mssql_inventory_table_data','mssql_count_model','recent_data','member_count','AlternativeAddressTable','NotesEntry'] 

    def db_for_read(self, model, **hints):
        if model.__name__ in self.mssql_tables:
            return 'mssql'
        return 'default'

    def db_for_write(self, model, **hints):
        if model.__name__ in self.mssql_tables:
            return 'mssql'
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        if obj1._state.db == obj2._state.db:
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if model_name in self.mssql_tables:
            return db == 'mssql'
        return db == 'default'
