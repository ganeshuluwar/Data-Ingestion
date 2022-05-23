import streamlit as st
import streamlit.components.v1 as stc
import mysql.connector
import os
import pandas as pd
import extract

# File Processing Pkgs
import pandas as pd
import docx2txt
from PIL import Image 
from PyPDF2 import PdfFileReader
import pdfplumber
from minio import Minio
from minio.error import S3Error


def main():

	client = Minio(
	    "10.0.2.15:9000",
	    access_key="minioadmin",
	    secret_key="minioadmin",
	    secure=False
	)
	# List buckets
	buckets = client.list_buckets()
	for bucket in buckets:
	    print('bucket:', bucket.name, bucket.creation_date)


	# Make 'asiatrip' bucket if not exist.
	found = client.bucket_exists("test")
	if not found:
	    client.make_bucket("test")
	else:
	    print("Bucket 'test' already exists")

	#User data

	st.title("User Data")
	    
	menu = ["Admin","Data Scientist","Analyst"]
	choice = st.selectbox("User",menu)
	name = st.text_input("Name")

	#Credentials to login to the database

	st.title("Login to the Database")

	hostname = st.text_input("Host Name")
	dbname = st.text_input("Database Name")
	username = st.text_input("User Name")
	userpassword = st.text_input("User Password")




	# Fxn Make Execution
	def sql_executor(raw_code):
	    client = Minio(
	    "10.0.2.15:9000",
	    access_key="minioadmin",
	    secret_key="minioadmin",
	    secure=False)

	    connection = mysql.connector.connect(host=hostname,
	            database=dbname, user=username, password=userpassword,
	            auth_plugin='mysql_native_password')
	    if connection.is_connected():
	        st.info('Database connnected')
	        cursor = connection.cursor()
	        cursor.execute(raw_code)
	        record = cursor.fetchall()
	        return record
	    
	    else:
	        st.info('Error while connecting to database')


	def read_pdf(file):
	    pdfReader = PdfFileReader(file)
	    count = pdfReader.numPages
	    all_page_text = ""
	    for i in range(count):
	        page = pdfReader.getPage(i)
	        all_page_text += page.extractText()

	    return all_page_text

	def read_pdf_with_pdfplumber(file):
	    with pdfplumber.open(file) as pdf:
	        page = pdf.pages[0]
	        return page.extract_text()

	# import fitz  # this is pymupdf

	# def read_pdf_with_fitz(file):
	#   with fitz.open(file) as doc:
	#       text = ""
	#       for page in doc:
	#           text += page.getText()
	#       return text 

	# Fxn

	@st.cache
	def load_image(image_file):
	    img = Image.open(image_file)
	    return img 


	#def save_uploadedfile(uploadedfile):
	     #with open(os.path.join(client,uploadedfile.name),"wb") as f:
	         #f.write(uploadedfile.getbuffer())
	         #print(os.getcwd())
	     #return st.success("Saved File:{} to tempDir".format(uploadedfile.name))


	def dataingestion():

	    menu = ['Structured Data']
	    choice = st.sidebar.selectbox('Upload from Database', menu)
	    if choice == 'Structured Data':
	        option = st.selectbox('Select the bucket', buckets)
	        options = (option)

	        dbname1 = st.text_input('Enter the new or existing bucketname')
	        client.make_bucket(dbname1)

	        st.subheader('Extract data from Database')
	        (col1, col2) = st.columns(2)
	        with col1:
	            with st.form(key='query_form'):
	                raw_code = st.text_area('Enter the Tablename')
	                prefix = 'select * from '
	                submit_code = st.form_submit_button('Execute')

	                    # Table of Info

	        with col2:

	            if submit_code:

	                                # st.info("Query Submitted")
	                                # st.code(prefix+raw_code+";")

	                                # Results

	                query_results = sql_executor(prefix + raw_code + ';')

	                with st.expander('RESULT'):
	                    query_df = pd.DataFrame(query_results)
	                    results = st.dataframe(query_df)
	                    query_df.to_csv(r'/home/sois/tempDir/export.csv',
	                                    index=False)

	                    pathh = r'/home/sois/tempDir/export.csv'
	                    
	                    
	                    	
	                    client.put_object(dbname1,'export.csv', pathh)
		            





	    menu = ["Image","Dataset","DocumentFiles"]
	    choice = st.sidebar.selectbox("Upload from local system",menu)






	    if choice == "Image":
	        st.subheader("Image")
	        image_file = st.file_uploader("Upload Image",type=['png','jpeg','jpg'])
	        if image_file is not None:
	        
	            # To See Details
	            # st.write(type(image_file))
	            # st.write(dir(image_file))
	            img = load_image(image_file)
	            file_details = {"Filename":image_file.name,"FileType":image_file.type,"FileSize":image_file.size}
	            st.write(file_details)
	            st.image(img,width=480)
	            with open(os.path.join("/home/sois/tempDir",image_file.name),"wb") as f: 
	                f.write(image_file.getbuffer())
	                st.success("Saved File")

	            path = "/home/sois/tempDir/"+image_file.name

	            client.fput_object("data",image_file.name, path)
	            print("'image_file' is successfully uploaded as " "object 'index.jpeg' to bucket 'data'.")
	            





	    elif choice == "Dataset":
	        st.subheader("Dataset")
	        data_file = st.file_uploader("Upload CSV",type=['csv'],accept_multiple_files=True)
	        if st.button("Process"):
	            if data_file is not None:
	                file_details = {"Filename":data_file.name,"FileType":data_file.type,"FileSize":data_file.size}
	                st.write(file_details)

	                df = pd.read_csv(data_file)
	                st.dataframe(df)

	    else: 
	        choice == "DocumentFiles"
	        st.subheader("DocumentFiles")
	        docx_file = st.file_uploader("Upload File",type=['txt','docx','pdf'],accept_multiple_files=True)
	        if st.button("Process"):
	            if docx_file is not None:
	                file_details = {"Filename":docx_file.name,"FileType":docx_file.type,"FileSize":docx_file.size}
	                st.write(file_details)
	                # Check File Type
	                if docx_file.type == "text/plain":
	                    # raw_text = docx_file.read() # read as bytes
	                    # st.write(raw_text)
	                    # st.text(raw_text) # fails
	                    st.text(str(docx_file.read(),"utf-8")) # empty
	                    raw_text = str(docx_file.read(),"utf-8") # works with st.text and st.write,used for futher processing
	                    # st.text(raw_text) # Works
	                    st.write(raw_text) # works
	                elif docx_file.type == "application/pdf":
	                    # raw_text = read_pdf(docx_file)
	                    # st.write(raw_text)
	                    try:
	                        with pdfplumber.open(docx_file) as pdf:
	                            page = pdf.pages[0]
	                            st.write(page.extract_text())
	                    except:
	                        st.write("None")
	                        
	                    
	                elif docx_file.type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
	                # Use the right file processor ( Docx,Docx2Text,etc)
	                    raw_text = docx2txt.process(docx_file) # Parse in the uploadFile Class directory
	                    st.write(raw_text)
	                    
	    
	if __name__ == '__main__':
		dataingestion()                   


if __name__ == '__main__':
    main()

