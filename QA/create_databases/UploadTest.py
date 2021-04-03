import datetime

from QA.PatentDatabaseTester import PatentDatabaseTester
from lib.configuration import get_current_config


class UploadTest(PatentDatabaseTester):
    def __init__(self, config):
        start_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        end_date = datetime.datetime.strptime(config['DATES']['END_DATE'], '%Y%m%d')
        super().__init__(config, 'TEMP_UPLOAD_DB', start_date, end_date)
        self.table_config = {
                'application':            {
                        'fields': {
                                'patent_id':                         {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'id_transformed':                    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'type':                              {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'number_transformed':                {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'number':                            {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'series_code_transformed_from_type': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'country':                           {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'id':                                {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'date':                              {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        }
                                }
                        },
                'botanic':                {
                        'custom_float_condition': "p.type='plant'",
                        'fields':                 {
                                'uuid':       {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id':  {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'latin_name': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'variety':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'figures':                {
                        'fields': {
                                'uuid':        {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id':   {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'num_figures': {
                                        'data_type':    'int',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'num_sheets':  {
                                        'data_type':    'int',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'foreigncitation':        {
                        'fields': {
                                'date':      {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'number':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'uuid':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'category':  {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'patent_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':  {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'foreign_priority':       {
                        'fields': {
                                'date':                {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'patent_id':           {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country':             {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'sequence':            {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country_transformed': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'kind':                {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'number':              {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'uuid':                {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'government_interest':    {
                        'fields': {
                                'gi_statement': {
                                        'data_type':    'mediumtext',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'ipcr':                   {
                        'fields': {
                                'ipc_version_indicator':      {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'ipc_class':                  {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'classification_value':       {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'uuid':                       {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':                   {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'subclass':                   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'classification_status':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'patent_id':                  {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'main_group':                 {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'classification_data_source': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'classification_level':       {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'subgroup':                   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'action_date':                {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     True
                                        },
                                'section':                    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'symbol_position':            {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        }
                                }
                        },
                'mainclass':              {
                        'fields': {
                                'id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'nber':                   {
                        'fields': {
                                'uuid':           {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'category_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'subcategory_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        }
                                }
                        },
                'nber_category':          {
                        'fields': {
                                'id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'title': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'nber_subcategory':       {
                        'fields': {
                                'id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'title': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'non_inventor_applicant': {
                        'fields': {
                                'fname':          {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'uuid':           {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'organization':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'patent_id':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':       {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'rawlocation_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'designation':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'lname':          {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'applicant_type': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        }
                                }
                        },
                'otherreference':         {
                        'fields': {
                                'uuid':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'text':      {
                                        'data_type':    'text',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':  {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'patent':                 {
                        'fields': {
                                'type':       {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'title':      {
                                        'data_type':    'mediumtext',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'number':     {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'kind':       {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'country':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'num_claims': {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'date':       {
                                        'data_type':    'date',
                                        'null_allowed': False,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'filename':   {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'id':         {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'abstract':   {
                                        'data_type':    'mediumtext',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'withdrawn':  {
                                        'data_type':    'int',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'pct_data':               {
                        'fields': {
                                'patent_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'kind':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'rel_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'doc_type':  {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'date':      {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                '102_date':  {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                '371_date':  {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'uuid':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        }
                                }
                        },
                'rawassignee':            {
                        'fields': {
                                'patent_id':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name_last':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'assignee_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'organization':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'rawlocation_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':       {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'type':           {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'uuid':           {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name_first':     {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'rawinventor':            {
                        'fields': {
                                'uuid':           {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name_last':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'patent_id':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':       {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'inventor_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'rule_47':        {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'rawlocation_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'deceased':       {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'name_first':     {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'rawlawyer':              {
                        'fields': {
                                'name_last':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'uuid':         {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'organization': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'lawyer_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'country':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'patent_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'sequence':     {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name_first':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'rawlocation':            {
                        'fields': {
                                'id':                      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country_transformed':     {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'location_id':             {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'location_id_transformed': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'city':                    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'state':                   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'country':                 {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        }
                                }
                        },
                'rel_app_text':           {
                        'fields': {
                                'sequence':  {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'uuid':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'text':      {
                                        'data_type':    'mediumtext',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'subclass':               {
                        'fields': {
                                'id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'usapplicationcitation':  {
                        'fields': {
                                'name':                       {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'sequence':                   {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'uuid':                       {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'kind':                       {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'application_id_transformed': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'patent_id':                  {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'number':                     {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'number_transformed':         {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'application_id':             {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country':                    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'date':                       {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'category':                   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        },
                'uspatentcitation':       {
                        'fields': {
                                'citation_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'category':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'date':        {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'sequence':    {
                                        'data_type':    'bigint',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'name':        {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'uuid':        {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'kind':        {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'patent_id':   {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country':     {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        }
                                }
                        },
                'uspc':                   {
                        'fields': {
                                'uuid':         {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'patent_id':    {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'mainclass_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'subclass_id':  {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     True
                                        },
                                'sequence':     {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        }
                                }
                        },
                'usreldoc':               {
                        'fields': {
                                'status':    {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'relkind':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'sequence':  {
                                        'data_type':    'int',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'reldocno':  {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'kind':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'uuid':      {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'country':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        },
                                'patent_id': {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'date':      {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'doctype':   {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     True
                                        }
                                }
                        },
                'us_term_of_grant':       {
                        'fields': {
                                'patent_id':       {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'term_extension':  {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'lapse_of_patent': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'disclaimer_date': {
                                        'data_type':    'date',
                                        'null_allowed': True,
                                        'date_field':   True,
                                        'category':     False
                                        },
                                'term_disclaimer': {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        },
                                'uuid':            {
                                        'data_type':    'varchar',
                                        'null_allowed': False,
                                        'category':     False
                                        },
                                'term_grant':      {
                                        'data_type':    'varchar',
                                        'null_allowed': True,
                                        'category':     False
                                        }
                                }
                        }
                }

    def test_patent_abstract_null(self, table):
        if not self.connection.open:
            self.connection.connect()
        count_query = "SELECT count(*) as null_abstract_count from {tbl} where abstract is null and type!='design' " \
                      "and type!='reissue'".format(
                tbl=table)
        with self.connection.cursor() as count_cursor:
            count_cursor.execute(count_query)
            count_value = count_cursor.fetchall()[0][0]
            if count_value != 0:
                raise Exception(
                        "NULLs (Non-design patents) encountered in table found:{database}.{table} column abstract. "
                        "Count: {count}".format(
                                database=self.config['PATENTSVIEW_DATABASES'][self.database_section], table=table,
                                count=count_value))

    def runTests(self):
        self.test_patent_abstract_null(table='patent')
        super(UploadTest, self).runTests()


if __name__ == '__main__':
    config = get_current_config('granted_patent', **{
            "execution_date": datetime.date(2020, 12, 29)
            })

    test_obj = UploadTest(config)

    test_obj.runTests()
