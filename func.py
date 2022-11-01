#
# Copyright (c) 2020 Oracle, Inc.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
#

import io
import os
import json
import sys
from fdk import response
import pandas as pd
from io import StringIO, BytesIO
import oci.object_storage

def handler(ctx, data: io.BytesIO=None):
    try:
        body = json.loads(data.getvalue())
        bucketName = body["bucketName"]
        fileName = body["fileName"]
    except Exception:
        raise Exception('Input a JSON object in the format: \'{"bucketName": "<bucket name>", "fileName": <file name>}\' ')
                                                
    resp = list_objects(bucketName, fileName)

    return response.Response(ctx, response_data=json.dumps(resp), headers={"Content-Type": "application/json"})

def list_objects(bucketName, fileName):
    
    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
        namespace = client.get_namespace().data
        print("Searching for objects in bucket " + bucketName, file=sys.stderr)
        obj = client.get_object(namespace, bucketName, fileName)
        dfStr = obj.data.text
        df = pd.read_csv(StringIO(dfStr))
        
        parquetIO = BytesIO()
        df.to_parquet(parquetIO, compression='gzip', engine='pyarrow')
        parquetIO.seek(0)
        parquetName = fileName.split('.')[0]+'.parquet'
        client.put_object(namespace, bucketName, parquetName, parquetIO)
        response = { "SUCCESS": parquetName + " was written to bucket " + bucketName }
    
    except Exception as e:
        response = { "ERROR": str(e)}
    return response
