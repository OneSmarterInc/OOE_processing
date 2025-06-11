from django.http import HttpResponse, Http404
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import get_object_or_404
from django.db import transaction
from .models import files,Archive,User,depnp,empyp,EDI_USER_DATA,elghp,Count_model
from rest_framework.decorators import api_view
import json
from io import BytesIO
import pyodbc
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from rest_framework import generics, status
from rest_framework.views import APIView
from rest_framework.response import Response
from .processinputfile import parse_edi_to_csv, send_success_email, send_error_email,parse_custodial_data
from rest_framework.parsers import MultiPartParser, FormParser
import shutil, os, re
from datetime import datetime
from rest_framework.permissions import AllowAny
from django.contrib.auth import authenticate
from .serializers import FilesSerializer,SignupSerializer,EmployeeSerializer, LoginSerializer,ArchiveSerializer,OTPLoginSerializer,CountSerializer
import pandas as pd
import tempfile
from django.core.files.storage import FileSystemStorage
import mimetypes
from django.conf import settings
from django.utils.encoding import smart_str
from django.utils.timezone import timedelta
from django.utils.timezone import now
from .checks import perform_checks
from .models import inventory_table_data,empyp,depnp,elghp

class SignupView(APIView):
    permission_classes = [AllowAny] 

    def post(self, request):
        serializer = SignupSerializer(data=request.data)
        if serializer.is_valid():
            user = serializer.save()
            return Response({'message': 'User created successfully', 'user': {'username': user.username, 'email': user.email}}, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class LoginView(APIView):
    permission_classes = [AllowAny] 

    def post(self, request):
        serializer = LoginSerializer(data=request.data)
        if serializer.is_valid():
            username = serializer.validated_data['username']
            password = serializer.validated_data['password']
            
            user = authenticate(username=username, password=password)
            if user:
                return Response({'message': 'Login successful', 'username': user.username}, status=status.HTTP_200_OK)
            else:
                return Response({'message': 'Invalid credentials'}, status=status.HTTP_401_UNAUTHORIZED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@csrf_exempt
def download_file(request):
    if request.method == 'POST':
        try:
          
            data = json.loads(request.body)
            file_id = data.get("id")
            if not file_id:
                return HttpResponse("File ID not provided", status=400)

            
            file_record = get_object_or_404(files, id=file_id)

      
            if not file_record.file_path:
                return HttpResponse("File path not set for the record.", status=404)

            file_path = file_record.file_path.path  

            #print(file_path)

            if os.path.isfile(file_path):
                pass
            else:
                print("Test",file_path)
                file_path = file_path.replace("\\media\\", "\\", 1)
                
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"File not found at path: {file_path}")

            
            file_name = file_record.file_name
            if not file_name.endswith('.csv'):
                file_name += '.csv'

            with open(file_path, 'rb') as f:
                response = HttpResponse(f.read(), content_type='text/csv')
                response['Content-Disposition'] = f'attachment; filename="{file_name}"'
                return response

        except json.JSONDecodeError:
            return HttpResponse("Invalid JSON format in the request body.", status=400)
        except FileNotFoundError as e:
            return HttpResponse(str(e), status=404)
        except Exception as e:
            return HttpResponse(f"An unexpected error occurred: {str(e)}", status=500)
    else:
        return HttpResponse("Method not allowed. Please use POST.", status=405)
    

@csrf_exempt
def download_excel_file(request):
    if request.method == 'POST':
        try:
          
            data = json.loads(request.body)
            file_id = data.get("id")
            if not file_id:
                return HttpResponse("File ID not provided", status=400)

            
            file_record = get_object_or_404(files, id=file_id)

      
            if not file_record.xlsx_file_path:
                return HttpResponse("File path not set for the record.", status=404)

            file_path = file_record.xlsx_file_path.path  

          
            if os.path.isfile(file_path):
                pass
            else:
                file_path = file_path.replace("\\media\\media\\", "\\media\\", 1)

            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"File not found at path: {file_path}")

            
            file_name = file_record.file_name
            if not file_name.endswith('.xlsx'):
                file_name += '.xlsx'

            with open(file_path, 'rb') as f:
                response = HttpResponse(f.read(), content_type='text/xlsx')
                response['Content-Disposition'] = f'attachment; filename="{file_name}"'
                return response

        except json.JSONDecodeError:
            return HttpResponse("Invalid JSON format in the request body.", status=400)
        except FileNotFoundError as e:
            return HttpResponse(str(e), status=404)
        except Exception as e:
            return HttpResponse(f"An unexpected error occurred: {str(e)}", status=500)
    else:
        return HttpResponse("Method not allowed. Please use POST.", status=405)


# @csrf_exempt
# def download_excel_file(request):
#     if request.method == 'POST':
#         try:
#             data = json.loads(request.body)
#             file_id = data.get("id")
#             if not file_id:
#                 return HttpResponse("File ID not provided", status=400)

#             file_record = get_object_or_404(files, id=file_id)
#             if not file_record.file_path:
#                 raise Http404("File not found")

#             file_path = file_record.file_path.path
#             print("aaaa",file_path)
#             if '\\media\\csv_files\\media\\csv_files\\' in file_path:
#                 file_path = file_path.replace('\\media\\csv_files\\media\\csv_files\\', '\\media\\csv_files\\',1)
#             elif '\\csv_files\\' not in file_path:
#                 file_path = file_path.replace("\\media\\", "\\media\\csv_files\\", 1)
#             try:
#                 try:
#                     file_path = file_path.replace("\\media\\media\\", "\\media\\", 1)
#                     df = pd.read_csv(file_path)
#                 except:
#                     corrected_file_path = file_path.replace("\\csv_files\\csv_files\\", "\\csv_files\\", 1)
#                     print(corrected_file_path,"jjjhhjj")
#                     df = pd.read_csv(corrected_file_path)
#             except FileNotFoundError:
#                 raise Http404("CSV file not found on the server.")

#             with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_file:
#                 temp_excel_path = tmp_file.name
#                 df.to_excel(temp_excel_path, index=False, engine="openpyxl")

#             with open(temp_excel_path, 'rb') as f:
#                 response = HttpResponse(
#                     f.read(),
#                     content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
#                 )
#                 response['Content-Disposition'] = f'attachment; filename="{file_record.file_name}.xlsx"'
            

#             os.remove(temp_excel_path)

#             return response

#         except json.JSONDecodeError:
#             return HttpResponse("Invalid JSON", status=400)

    #return HttpResponse("Method not allowed", status=405)

def download_input_file(request, file_id):
    print("Akshay")
    file_instance = get_object_or_404(files, id=file_id)
    print("Agrawal",file_instance)
    file_path = file_instance.input_file_path.path
    print("Test",file_path)
    if "\\EDI-Backend\\" not in file_path:
        print('hii')
        file_path = file_path.replace("\\media\\","\\EDI-Backend\\media\\",1)
    print(file_path)
    if not file_path.startswith(settings.MEDIA_ROOT):
        raise Http404("File path is invalid or not within the MEDIA_ROOT")

    if not os.path.exists(file_path):
        raise Http404("File does not exist")

    file_name = os.path.basename(file_path)
    mime_type, _ = mimetypes.guess_type(file_path)

    response = HttpResponse(open(file_path, 'rb'), content_type=mime_type)
    response['Content-Disposition'] = f'attachment; filename="{file_name}"'
    return response


class FilesListView(generics.ListAPIView):
    queryset = files.objects.all()
    serializer_class = FilesSerializer

class FilesFilterView(APIView):
    def get(self, request):
        file_type = request.query_params.get('file_type')
        file_date = request.query_params.get('file_date')
        print(file_date,file_type)

        if file_type is None or file_date is None:
            return Response(
                {"error": "Both file_type and file_date are required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        files_filtered = files.objects.filter(file_type=file_type, file_date=file_date)
        print(files_filtered)

        serializer = FilesSerializer(files_filtered, many=True)
        return Response(serializer.data)
    

class ArchiveListView(generics.ListAPIView):
    queryset = Archive.objects.all()
    serializer_class = ArchiveSerializer    

class ArchiveFilterView(APIView):
    def get(self, request):

        file_type = request.query_params.get('file_type')
        file_date = request.query_params.get('file_date')
        print(file_date,file_type)


        if file_type is None or file_date is None:
            return Response(
                {"error": "Both file_type and file_date are required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        files_filtered = Archive.objects.filter(file_type=file_type, file_date=file_date)
        print(files_filtered)
        serializer = ArchiveSerializer(files_filtered, many=True)
        return Response(serializer.data)
    
    
output_folder = r"C:\Users\avina\OneDrive\Desktop\Output"
system_folder = r"C:\Users\avina\OneDrive\Desktop\edi-backend\edi\media\csv_files"
archive_folder = r"C:\Users\avina\OneDrive\Desktop\Archive"
from dateutil import parser
class FileUploadView(APIView):
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, format=None):
        file = request.FILES.get('file')
        email = request.data.get('email', 'avinashkalmegh93@gmail.com')

        if not file:
            return Response({"message": "No file provided"}, status=status.HTTP_400_BAD_REQUEST)

        file_name = file.name
        storage = FileSystemStorage(location='media/input_files/')
        saved_file_name = storage.save(file.name, file)
        saved_file_path = storage.path(saved_file_name)

        if True:
            match = re.match(r"^EDI_(\d{3})_(\d{2}-\d{2}-\d{4})$", os.path.splitext(file_name)[0])
            if not match:
                pass
                # return Response({"message": "Invalid file name format. Expected format: EDI_XXX_MM-DD-YYYY"}, 
                #                 status=status.HTTP_400_BAD_REQUEST)

            file_type = ""
            file_date_str = ""
            file_date = 23-11-2024

            output_folder = "media/csv_files/"
            J=output_folder
            archive_folder = "media/archive/"
            os.makedirs(output_folder, exist_ok=True)
            os.makedirs(archive_folder, exist_ok=True)

            input_file_path = os.path.join(output_folder, file_name)
            with open(input_file_path, 'wb+') as destination:
                for chunk in file.chunks():
                    destination.write(chunk)
           
            output_file_path,edi_segment_paths = parse_edi_to_csv(input_file_path, output_folder,J)
            insert_df = pd.read_csv(output_file_path)
            insert_df['marital_status'] = None
            spouse_ssns = insert_df.loc[insert_df['SUB/DEP'] == 'Spouse', 'SSN'].unique()
            insert_df.loc[(insert_df['SSN'].isin(spouse_ssns)) & (insert_df['SUB/DEP'] == 'Subscriber'), 'marital_status'] = 'Married'
            insert_df['CLIENT'] = 'OOE'
            insert_df['EFF DATE'] = pd.to_datetime(insert_df['EFF DATE'])

            insert_df['eff_date_day'] = insert_df['EFF DATE'].dt.day
            insert_df['eff_date_month'] = insert_df['EFF DATE'].dt.month
            insert_df['eff_date_year'] = insert_df['EFF DATE'].dt.year
            eligibility = []
            for ind,rx in insert_df.iterrows():
                if rx['SUB/DEP'] == "Subsciber":
                    eli = elghp(ELSSN = rx['SSN'],ELPLAN=rx['PLAN'],ELCLAS=rx['CLASS'],ELCLNT=rx['CLIENT'],ELEPDY=rx['eff_date_year'],ELEPDM=rx['eff_date_month'],ELEPDD=rx['eff_date_day'])
                    eligibility.append(eli)
                else:
                    eli = elghp(ELSSN = rx['DEP SSN'],ELPLAN=rx['PLAN'],ELCLAS=rx['CLASS'],ELCLNT=rx['CLIENT'],ELEPDY=rx['eff_date_year'],ELEPDM=rx['eff_date_month'],ELEPDD=rx['eff_date_day'])
                    eligibility.append(eli)
            elghp.objects.bulk_create(eligibility)
            filtered_df = insert_df[insert_df['SUB/DEP'] != 'Subscriber']
            if "ADDRESS 2" not in filtered_df.columns:
                filtered_df['ADDRESS 2'] = ''
            updated_insert_df = filtered_df[["FIRST NAME","SSN","SEX","DOB","ADDRESS 1","ADDRESS 2","CITY","STATE","ZIP",'CLIENT']]
            subscribers = []
            def convert_to_date_components(date_string):
                try:
                    parsed_date = parser.parse(date_string)
                    formatted_date = parsed_date.strftime("%Y-%m-%d")
                    return {
                        "formatted_date": formatted_date,
                        "year": parsed_date.year,
                        "month": parsed_date.month,
                        "day": parsed_date.day,
                    }
                except (parser.ParserError, TypeError, ValueError):
                    return None
            for index, row in updated_insert_df.iterrows():
                date = row['DOB']
                obj = convert_to_date_components(date)
                try:
                    year = obj['year']
                    month = obj['month']
                    day = obj['day']
                except:
                    year = 2024
                    month = 12
                    day = 31
                subscriber = depnp(DPDOBY=year, DPDOBM=month,DPDOBD=day,DPSEX=row['SEX'],DPSSN=row['SSN'],DEPCLNT=row['CLIENT'])
                subscribers.append(subscriber)
            depnp.objects.bulk_create(subscribers)
            new_filtered_df = insert_df[insert_df['SUB/DEP'] == 'Subscriber']
            if "ADDRESS 2" not in new_filtered_df.columns:
                new_filtered_df['ADDRESS 2'] = ''
            new_updated_insert_df = new_filtered_df[["FIRST NAME","SSN","SEX","DOB","ADDRESS 1","ADDRESS 2","CITY","STATE","ZIP",'CLIENT','marital_status']]
            new_subscribers = []
            for index, row in new_updated_insert_df.iterrows():
                date = row['DOB']
                obj = convert_to_date_components(date)
                year = obj['year']
                month = obj['month']
                day = obj['day']
                subscriber = empyp(EMDOBY=year, EMDOBM=month,EMDOBD=day,EMSEX=row['SEX'],EMSSN=row['SSN'],EMCITY=row['CITY'],EMST=row['STATE'],EMZIP5=row['ZIP'],EMADR1=row['ADDRESS 1'],EMADR2=row["ADDRESS 2"],EMCLNT=row['CLIENT'],EMMS=row['martial_status'])
                new_subscribers.append(subscriber)
            empyp.objects.bulk_create(new_subscribers)
            shutil.move(input_file_path, os.path.join(archive_folder, file_name))
            pth = os.path.join(output_folder, f"{os.path.splitext(file_name)[0]}.csv")
            pth_xlsx =  os.path.join(output_folder, f"{os.path.splitext(file_name)[0]}.xlsx")
            file_record = files.objects.create(
                file_name=file_name,
                file_type=file_type,
                file_date=file_date,
                file_path=pth,
                xlsx_file_path = pth_xlsx,
                created_by="API",
                upload_status=True,
                email_sent_status=True,
                email_sent_to=email,
                input_file_path=saved_file_path,
                edi_segment_path = edi_segment_paths
            )

            # send_success_email(email, file_name, output_file_path)
            return Response({"message": "File processed successfully"}, status=status.HTTP_200_OK)

        # except Exception as e:
        #     send_error_email(email, file_name, str(e))
        #     return Response({"message": f"Error processing file: {str(e)}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        
        


smtp_config = {
    'host': 'mail.privateemail.com',
    'port': 465,
    'user': 'support@disruptionsim.com',
    'password': 'Onesmarter@2023'
}

   
def send_mail(email,otp):
    
    try:
        server = smtplib.SMTP_SSL(smtp_config['host'], smtp_config['port'])
        server.login(smtp_config['user'], smtp_config['password'])

        msg = MIMEMultipart()
        msg['From'] = smtp_config['user']
        msg['To'] = email
        msg['Subject'] = 'Your OSI Pay OTP for Secure Login'

       
        body = f"""
        <p>Dear User,</p>
        <p>Thank you for using OSI Pay. For added security, we have implemented a two-factor authentication process. Please use the following One-Time Password (OTP) to complete your login:</p>
        <p>Your OTP: {otp}</p>
        <p>This OTP is valid for the next 10 minutes. If you did not request this, please disregard this email.</p>
        <p>Thank you for choosing OSI Pay.</p>
        <p></p>
        <p></p>
        <p>Best regards,</p>
        <p>The OSI Pay Team</p>
        """

        msg.attach(MIMEText(body, 'html'))
        # Include the main recipient and the CC in the recipients list
        recipients = email
        server.send_message(msg, from_addr=smtp_config['user'], to_addrs=recipients)
        server.quit()
        print("Email sent successfully")
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False 

class SendOTPView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        email = request.data.get("email")
        password = request.data.get("password")

        if not email or not password:
            return Response({"error": "Email and password are required"}, status=status.HTTP_400_BAD_REQUEST)

        # Authenticate user with email and password
        user = authenticate(username=email, password=password)
        if not user:
            return Response({"error": "Invalid email or password"}, status=status.HTTP_401_UNAUTHORIZED)

        # Generate OTP and send email
        otp = user.generate_otp()
        send_mail(
           email,
           otp
        )
        return Response({"message": "OTP sent successfully"}, status=status.HTTP_200_OK)
        
        
        
class OTPLoginView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = OTPLoginSerializer(data=request.data)
        if serializer.is_valid():
            email = serializer.validated_data["email"]
            otp = serializer.validated_data["otp"]

            try:
                user = User.objects.get(email=email, otp=otp)
                # Check if OTP is expired (valid for 5 minutes)
                if now() - user.last_otp_sent > timedelta(minutes=5):
                    return Response({"error": "OTP has expired"}, status=status.HTTP_400_BAD_REQUEST)

                # Successful login
                user.otp = None  # Clear OTP after use
                user.save()
                return Response({"message": "Login successful", "username": user.username}, status=status.HTTP_200_OK)
            except User.DoesNotExist:
                return Response({"error": "Invalid OTP or email"}, status=status.HTTP_401_UNAUTHORIZED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)        


@csrf_exempt
def Download_excel(request):
    if request.method == 'POST':
        try:
          
            data = json.loads(request.body)
            file_id = data.get("id")
            if not file_id:
                return HttpResponse("File ID not provided", status=400)

            
            file_record = get_object_or_404(files, id=file_id)

      
            if not file_record.xlsx_file_path:
                return HttpResponse("File path not set for the record.", status=404)

            file_path = file_record.edi_segment_path.path  

          
            if os.path.isfile(file_path):
                pass
            else:
                file_path = file_path.replace("\\media\\media\\", "\\media\\", 1)

            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"File not found at path: {file_path}")

            
            file_name = file_record.file_name
            if not file_name.endswith('.xlsx'):
                file_name += '.xlsx'

            with open(file_path, 'rb') as f:
                response = HttpResponse(f.read(), content_type='text/xlsx')
                response['Content-Disposition'] = f'attachment; filename="{file_name}"'
                return response

        except json.JSONDecodeError:
            return HttpResponse("Invalid JSON format in the request body.", status=400)
        except FileNotFoundError as e:
            return HttpResponse(str(e), status=404)
        except Exception as e:
            return HttpResponse(f"An unexpected error occurred: {str(e)}", status=500)
    else:
        return HttpResponse("Method not allowed. Please use POST.", status=405)

# class Download_excel(APIView):
#   def get(self, request, *args, **kwargs):
#     date = request.query_params.get('date')
#     print(date)
#     connection = pyodbc.connect(
#         'DRIVER={ODBC Driver 17 for SQL Server};'
#         'SERVER=ABCCOLUMBUSSQL2;'
#         'DATABASE=EDIDATABASE;'
#         'UID=sa;'
#         'PWD=ChangeMe#2024;'
#     )
#     cursor = connection.cursor()
#     query = "SELECT * FROM edisegmentable WHERE Date_edi = ?"
#     cursor.execute(query, ("2024-12-02",))
#     rows = cursor.fetchall()
#     columns = [column[0] for column in cursor.description]
#     print('wisper')
#     df = pd.DataFrame.from_records(rows, columns=columns)
#     cursor.close()
#     connection.close()
#     print('fetching done')
#     # validations = perform_checks(df)
#     # validations_df = pd.DataFrame(validations, columns=['Validation Errors'])

#     output_folder = 'media/output_excels/' 
#     os.makedirs(output_folder, exist_ok=True)

#     workbook_path = os.path.join(output_folder, f'data_with_validations_{"Sample"}.xlsx')
#     with pd.ExcelWriter(workbook_path, engine='xlsxwriter') as writer:
#       df.to_excel(writer, index=False, sheet_name='DataFrame')
#     #   validations_df.to_excel(writer, index=False, sheet_name='Validations')

#     with open(workbook_path, 'rb') as excel_file:
#       response = HttpResponse(excel_file.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
#       response['Content-Disposition'] = f'attachment; filename="{os.path.basename(workbook_path)}"' 
#       return response
    

class Download_edi_Custodial_xlsx(APIView):
    def get(self, request):
        date = request.query_params.get('date')
        if not date:
            return HttpResponse("Date parameter is missing", status=400)

        output_folder = "media/csv_files/"
        os.makedirs(output_folder, exist_ok=True)

        filtered_data = EDI_USER_DATA.objects.filter(date_edi=date)
        if not filtered_data.exists():
            return HttpResponse("No data found for the given date", status=404)

        db_df = pd.DataFrame.from_records(filtered_data.values())
        db_df.drop(columns=['temp_ssn'],inplace=True)
        
        excel_filename = os.path.join(output_folder, f"edi_data_{date}.xlsx")
        db_df.to_excel(excel_filename, index=False)
        with open(excel_filename, 'rb') as excel_file:
            response = HttpResponse(excel_file, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename=custodial_data_{date}.xlsx'
        return response
    
class Download_edi_Custodial_csv(APIView):
    def get(self, request):
        date = request.query_params.get('date')
        if not date:
            return HttpResponse("Date parameter is missing", status=400)

        output_folder = "media/csv_files/"
        os.makedirs(output_folder, exist_ok=True)

        filtered_data = EDI_USER_DATA.objects.filter(date_edi=date)
        if not filtered_data.exists():
            return HttpResponse("No data found for the given date", status=404)

        db_df = pd.DataFrame.from_records(filtered_data.values())
        db_df.drop(columns=['temp_ssn'],inplace=True)
        csv_data = db_df.to_dict(orient='records')
        cus_df = parse_custodial_data(csv_data)
        excel_filename = os.path.join(output_folder, f"edi_data_{date}.csv")
        cus_df.to_excel(excel_filename, index=False)
        with open(excel_filename, 'rb') as excel_file:
            response = HttpResponse(excel_file, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename=custodial_data_{date}.csv'
        return response

from openpyxl import Workbook
def export_inventory_data(request):
    employees = inventory_table_data.objects.all()
    field_names = [field.name for field in inventory_table_data._meta.get_fields()]

    wb = Workbook()
    ws = wb.active
    ws.title = "Inventory Data"

    ws.append(field_names)
    for emp in employees:
        row_data = [getattr(emp, field) for field in field_names]
        ws.append(row_data)

    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=inventory_data.xlsx'
    wb.save(response)

    return response


class Get_Count(APIView):
    def post(self,request):
        data = request.data
        date = data.get('date')
        db_data = Count_model.objects.get(date=date)
        serializer  = CountSerializer(db_data)
        if serializer.is_valid():
            return Response(serializer.data)
        else:
            return Response("Data not found")
        

@api_view(['GET'])
def search_employee(request):
    query = request.GET.get('query', '')

    employees = empyp.objects.filter(EMNAME__icontains=query)

    serializer = EmployeeSerializer(employees, many=True)
    
    return Response({"count": employees.count(), "results": serializer.data})


@api_view(['POST'])
def add_member(request):
    # Validate Relationship field
    relationship = request.data.get("Relationship")
    if relationship != "Member":
        return Response(
            {"error": "Only members can be added. Invalid relationship type."}, 
            
        )
    # Extract Fields (SSN is mandatory, others are optional)
    EMSSN = request.data.get("EMSSN")  # Social Security Number (Required)
    if not EMSSN:
        return Response({"error": "SSN (EMSSN) is required."}, status=400)
    
    if empyp.objects.filter(EMSSN=EMSSN).exists():
            return Response({"error": "SSN already exists. Duplicate entries are not allowed."}, status=400)


    try:
        # Extracting fields exactly as per database
        EMNAME = request.data.get("EMNAME")  # Full Name
        EMSSN = request.data.get("EMSSN")  # Social Security Number
        EMSEX = request.data.get("EMSEX")  # Gender
        EMDOB = request.data.get("EMDOB")  # Date of Birth (YYYY-MM-DD)
        EMADR1 = request.data.get("EMADR1")  # Address
        EMCITY = request.data.get("EMCITY")  # City
        EMST = request.data.get("EMST")  # State
        Country = request.data.get("Country")  # Country (To be merged with Address)
        EMMEM = request.data.get("EMMEM")  # Member ID

        ELPLAN = request.data.get("ELPLAN")  # Plan
        ELCLAS = request.data.get("ELCLAS")  # Class

        # Convert date format
        dob_parsed = datetime.strptime(EMDOB, "%Y-%m-%d") if EMDOB else None

        # Merge Country with Address
        full_address = f"{EMADR1}, {EMCITY}, {EMST}, {Country}".strip()

        # Store in empyp Table
        empyp_obj, created = empyp.objects.update_or_create(
            EMSSN=EMSSN,
            defaults={
                "EMNAME": EMNAME,
                "EMSEX": EMSEX,
                "EMDOBY": dob_parsed.year if dob_parsed else None,
                "EMDOBM": dob_parsed.month if dob_parsed else None,
                "EMDOBD": dob_parsed.day if dob_parsed else None,
                "EMADR1": full_address,  # Storing Country inside Address
                "EMCITY": EMCITY,
                "EMST": EMST,
                "EMMEM": EMMEM
            }
        )

        # Store in elghp Table (linked via SSN)
        elghp_obj, created = elghp.objects.update_or_create(
            ELSSN=EMSSN,  # Mapping correctly
            defaults={
                "ELPLAN": ELPLAN,
                "ELCLAS": ELCLAS
            }
        )

        return Response({"message": "Member added successfully!", "status": "success"})
    
    except Exception as e:
        return Response({"error": str(e)}, status=400)
        
class GetMemberInfo(APIView):
    def get(self, request):
        name = request.GET.get('name')
        relationship = request.GET.get('relationship')
        ssn = request.GET.get('ssn')
        
        if not name or not relationship or not ssn:
            return Response({'error': 'Missing required parameters'}, status=400)
        
        if relationship.lower() == "member":
            member = empyp.objects.filter(EMSSN=ssn, EMNAME__icontains=name).first()
            if not member:
                return Response({'error': 'Member not found'}, status=404)
            
            
            dob = self.format_dob(member.EMDOBM, member.EMDOBD, member.EMDOBY)
        
        
            data = {
                 "name": member.EMNAME if member.EMNAME else "name is not available",
                "ssn": member.EMSSN if member.EMSSN else "SSN is not available",
                "relationship": relationship,
                "member_id": member.EMMEM if member.EMMEM else "member ID is not available",
                "dob": dob if dob else "DOB is not available",
                "address": member.EMADR1 if member.EMADR1 else "address is not available",
                "state": member.EMST if member.EMST else "state is not available",
                "city": member.EMCITY if member.EMCITY else "city is not available",
                "country": "USA"
            } 
        
        else:
            
            dependent = depnp.objects.filter(DPDSSN=ssn, DPNAME__icontains=name).first()
            if not dependent:
                return Response({"error": "Dependent not found"}, status=404)

            
            dob = self.format_dob(dependent.DPDOBM, dependent.DPDOBD, dependent.DPDOBY)

            #EMPYP
            member = empyp.objects.filter(EMSSN=dependent.DPSSN).first()

            data = {
                "name": dependent.DPNAME if dependent.DPNAME else "name is not available",
                "ssn": dependent.DPDSSN if dependent.DPDSSN else "SSN is not available",
                "relationship": dependent.DPTYPE if dependent.DPTYPE else "relationship is not available",
                "member_id": member.EMMEM if member and member.EMMEM else "member ID is not available",
                "dob": dob if dob else "DOB is not available",
                "address": member.EMADR1 if member and member.EMADR1 else "address is not available",
                "state": member.EMST if member and member.EMST else "state is not available",
                "city": member.EMCITY if member and member.EMCITY else "city is not available",
                "country": "USA"
            }

        return Response(data)      
    
    def format_dob(self, month, day, year):
        if not (month and day and year):
            return None
        try:
            dob = datetime(year, month, day)
            return dob.strftime("%B %d, %Y")
        except ValueError:
            return None
        

class UpdateMemberInfo(APIView):
    def post(self, request):
        # Get fields from the POSTed JSON payload
        name = request.data.get('name')
        relationship = request.data.get('relationship')
        ssn = request.data.get('ssn')
        member_id = request.data.get('member_id')
        dob_str = request.data.get('dob')  # Expected format: "April 15, 1986"
        address = request.data.get('address')
        state = request.data.get('state')
        city = request.data.get('city')
        # country is default 'USA', so we do not need to update it

        # Check that all required fields are provided
        if not all([name, relationship, ssn, member_id, dob_str, address, state, city]):
            return Response({"error": "Missing required fields"}, status=400)

        # Parse the formatted date string into year, month, day
        year, month, day = self.parse_dob(dob_str)
        if not all([year, month, day]):
            return Response({"error": "Invalid date format. Expected format like 'April 15, 1986'"}, status=400)

        if relationship.lower() == "member":
            # Find the member record in empyp table
            instance = empyp.objects.filter(EMSSN=ssn, EMNAME__icontains=name).first()
            if not instance:
                return Response({"error": "Member record not found"}, status=404)
            # Update the member's fields
            instance.EMNAME = name
            instance.EMSSN = ssn
            instance.EMMEM = member_id
            instance.EMDOBY = year
            instance.EMDOBM = month
            instance.EMDOBD = day
            instance.EMADR1 = address
            instance.EMST = state
            instance.EMCITY = city
            instance.save()
            return Response({"message": "Member record updated successfully"})

        else:
            # For dependent records in depnp
            instance = depnp.objects.filter(DPDSSN=ssn, DPNAME__icontains=name).first()
            if not instance:
                return Response({"error": "Dependent record not found"}, status=404)
            
            print("Before Update:", instance.__dict__)
            # Update dependent's fields
            with transaction.atomic():
                rows_updated = depnp.objects.filter(id=instance.id).update(
                    DPNAME=name,
                    DPDSSN=ssn,
                    DPTYPE=relationship,
                    DPDOBY=year,
                    DPDOBM=month,
                    DPDOBD=day,
                )
            # If your design also requires updating member details for dependents,
            # you might perform an additional update here (e.g., if you have a field for member's SSN)
            # with transaction.atomic():
            #     instance.save()
            
            year, month, day = self.parse_dob(dob_str)
            print("Parsed DOB:", year, month, day)

            
            if rows_updated == 0:
                return Response({"error": "Update failed!"}, status=500)
            else:
                # Optionally re-fetch instance to verify changes:
                updated_instance = depnp.objects.get(id=instance.id)
                print("After Update (using update()):", updated_instance.__dict__)
                return Response({"message": "Dependent record updated successfully"})

    def parse_dob(self, dob_str):
        """
        Parse a DOB string formatted as "April 15, 1986" into (year, month, day).
        Returns a tuple of (year, month, day) or (None, None, None) if parsing fails.
        """
        try:
            dt = datetime.strptime(dob_str, "%B %d, %Y")
            return dt.year, dt.month, dt.day
        except Exception:
            return None, None, None



