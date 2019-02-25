# Omar's Python/Boto/Athena/SQL Jack-knife carpenter library of tools to data engineering useful tables for DS/AS
#
# Right now, Sagemaker/Rancher using AWS credentials
# Could run these on AWS Lambda
# Docker image created and run on ECS instance

try:
    import boto3
except:
    print('Installing boto3')
    get_ipython().system('pip install boto3')
    import boto3
import numpy as np
import time

class JackKnifeDataEngineer(object):
    #listAllBuckets(s3) --> dir, ls for s3 buckets only
    #listSubFolders(bucket,parent) --> recursive cd, ls for s3 buckets only
    #listContents(bucket,parent) --> list content in bucket (files)
    #readContents(bucket,parent,key) --> finds and downloads file from s3
    #run_query(query,database,s3_output) --> init athena client, runs whatever is in query (up to 4 queries)
    #checkQueries(is_queries_running,queryStore,repeatTime) --> gets status of current queries, repeats every x seconds if set > 0
    #fetchAppsNames(override) --> checks if exists already in local store, calls getAppsTable if not present or if override=True
    #getCompanyAppsTable() grabs current apps_id, apps_name list
    
    def __init__(self, database = "appsresults", 
                       bucketrino = 'aws-athena-query-results-XXXXXXXXXXXXXX',
                       locationrino = 'UserAppsActivity/'):
#     def __init__(self, bucket, sub_bucket):
        self.database = database
#         self.bucketrino = bucketrino
#         self.locationrino = locationrino
        self.athena, self.s3, self.s3_client, self.s3_output, self.database, self.locationrino, self.bucketrino = self._initiateClient(database = database, 
                       bucketrino = bucketrino,
                       locationrino = locationrino)
        self.explicit = True

            
#         s3 = boto3.resource('s3')
#         self.locationrino = sub_bucket
#         self.bucketrino = bucket
#         self.s3_output = 's3://'+bucketrino+'/'+locationrino+'/'

    def _getCredentials(self):
        #Point to wherever your credentials are. Fucnction for exporting credentials was as follows:        
        #         a = {'aws_access_key_id':'XXXXXXXXXXXXXX', 
        #          'aws_secret_access_key':'123412342134123412xxxxxxxxxxxx',
        #         'region_name':'us-east-1'}
        #         import json
        #         with open('mycredentials', 'w') as f:
        #             json.dump(a,f)

        import os
    #     cur_path = os.path.dirname(os.path.abspath(__file__))
        dir_path = os.getcwd()
        os.system('cd ~\\omar\\credentials\\')
        file = 'mycredentials'
        import json
        with open(file) as f:
            a=json.load(f)

        os.system('cd ' + dir_path)
        return a['aws_access_key_id'], a['aws_secret_access_key'], a['region_name']
        

    def _initiateClient(self, database = "appsresults", 
                       bucketrino = 'aws-athena-query-results-XXXXXXXXXXXXXX',
                       locationrino = 'UserAppsActivity/'):    
        #define database to query, and s3 location for query dump
#         import boto3
        s3_output = 's3://'+bucketrino+'/'+locationrino+'/'
        akey, skey, rn = self._getCredentials()
        s3 = boto3.resource('s3', 
                              aws_access_key_id=akey, 
                              aws_secret_access_key=skey, 
                              region_name=rn
                              )
        #low level client representing Athena, to check on queries
        # boto3.setup_default_session(region_name='us-east-1')
        athena = boto3.client('athena', 
                              aws_access_key_id=akey, 
                              aws_secret_access_key=skey, 
                              region_name=rn
                              )
        s3_client = boto3.client('s3', 
                              aws_access_key_id=akey, 
                              aws_secret_access_key=skey, 
                              region_name=rn)
        return athena, s3, s3_client, s3_output, database, locationrino, bucketrino

    def changeOutput(self, verboseTrigger = None):
        #flips switch for verbose output, otherwise you can just set it directly changeOutput(True/False)
        if verboseTrigger == None:
            if self.explicit:
                self.explicit = False
                print("No more output. Stealth mode, activated.")
            else:
                self.explicit = True
                print("Output on. Let the rambling commence.")
        else:
            if verboseTrigger:
                self.explicit = True
                print("Output on. Let the rambling commence.")
            else:
                self.explicit = False
                print("No more output. Stealth mode, activated.")
            
            
    def listAllBuckets(self):
        for bucket in self.s3.buckets.all():
            if self.explicit:
                print(bucket.name)
            
    def changeBucket(self, new_bucket):
        self.bucketrino = new_bucket
        self.s3_output = 's3://'+self.bucketrino+'/'+self.locationrino+'/'

    def changeSubfolder(self, new_location):
        self.locationrino = new_location
        self.s3_output = 's3://'+self.bucketrino+'/'+self.locationrino+'/'
        
    def changeDatabase(self, new_database):
        self.database = new_database

    def listSubFolders(self, bucket=None, parent = None, verboseTrigger=True):
        listSubfolders = []
        if bucket == None:
            bucket = self.bucketrino
        if parent == None:
            parent = self.locationrino
    #listSubFolders('aws-athena-query-results-XXXXXXXXXXXXXX','Unsaved/2018/01/')
#         client = boto3.client('s3')
        result = self.s3_client.list_objects(Bucket = bucket, 
                                     Prefix=parent, Delimiter='/')
        for o in result.get('CommonPrefixes'):
            if self.explicit and verboseTrigger:
                print('sub folder : ', o.get('Prefix'))
            listSubfolders.append(o.get('Prefix'))
        return listSubfolders

    def listContents(self, bucket=None, parent = None, verboseTrigger = True):
        listFiles = []
        if bucket == None:
            bucket = self.bucketrino
        if parent == None:
            parent = self.locationrino        
    #    parent = "folderone/foldertwo/"
        if self.explicit:
            print('Looking for file. ')
#         s3 = boto3.resource('s3')
        bucket = self.s3.Bucket(name=bucket)
        FilesNotFound = True
        for obj in bucket.objects.filter(Prefix=parent):
             if self.explicit and verboseTrigger:
                    print('{0}:{1}'.format(bucket.name, obj.key))
             FilesNotFound = False
             listFiles.append(obj.key)
        if FilesNotFound:
            if verboseTrigger:
                 print("ALERT", "No file in {0}/{1}".format(bucket, parent))
        return listFiles

    def readContents(self,parent,key,bucket=None):
        if bucket == None:
            bucket = self.bucketrino
#         s3 = boto3.resource('s3')
        s3bucket = self.s3.Bucket(name=bucket)
        actualkey = parent+'/'+key
        path =key
    #    print(actualkey)
        FilesNotFound = True
        for obj in s3bucket.objects.filter(Prefix=parent):
    #        print(obj.key)
            if obj.key == actualkey:
                if self.explicit:
                    print("Found it! Good job on getting all those numbers and letter in the right order.")
                FilesNotFound=False

                if self.explicit:
                    print("tryna download " +actualkey+ " to "+path +"\n")
                self.s3.Bucket(bucket).download_file(actualkey, path)
                if self.explicit:
                    print("download complete")

        if FilesNotFound:
             print("ALERT", "Searched: {0} and found NO file named {1}/{2}".format(bucket, parent, key))

    def moveContents(self, from_parent, to_parent,  from_bucket=None, to_bucket=None, SAFETY_CAP = 2):
        if from_bucket==None:
            from_bucket = self.bucketrino
        if to_bucket==None:
            to_bucket = self.bucketrino
        from_loc = from_bucket + '/' + from_parent
        to_loc = to_bucket + '/' + to_parent
#         s3 = boto3.resource('s3')
#         s3_source = self.s3_client.Bucket(name=from_bucket)
#         s3_destination = self.s3_client.Bucket(name=to_bucket)
        FilesNotFound = True
        SAFETY_TRIGGER = 0
#         for obj in s3bucket.objects.filter(Prefix=parent):
#         s3bucket = self.s3.Bucket(name=from_bucket)
#         for obj in s3bucket.objects.filter(Prefix=from_parent):
        fileCount = 0
        s3_list = self.s3.Bucket(name=from_bucket).objects.filter(Prefix=from_parent)
        for obj in s3_list:
            fileCount +=1
#             print(obj.key)
#             print(obj.bucket_name)
#             print(from_parent)
#             print(to_parent)
#             print(to_parent + obj.key[len(from_parent):])
#             print(from_loc)
            if SAFETY_TRIGGER < SAFETY_CAP:
        #        print(obj.key)
                if FilesNotFound:
                    if self.explicit:
                        print("Found some objects...movin' them.")
                    FilesNotFound=False
                #copy
                print('copying ' + str(obj.key) + ' to ' + str(to_parent + obj.key[len(from_parent):]))
                self.s3_client.copy_object(CopySource = {'Bucket': from_bucket, 'Key': obj.key}, 
                                           Bucket = to_bucket, 
                                           Key = to_parent + obj.key[len(from_parent):])
                    #strips away the home directory from key name (which includes home dir) and adds on new intended dir (to_parent)
    #             s3_destination.copy_key(obj.key.name, s3_source.name, obj.key.name)
                #delete
                print('deleting ' + str(obj.key))
                obj.delete()
                SAFETY_TRIGGER +=1
            else:
                print('MORE THAN '+str(SAFETY_CAP)+' file(s)...skipping, ENABLING SAFETY TRIGGER. Set changeOutput(True) to make sure' 
                      + ' you are not deleting unintentional objects. Otherwsie, set safety trigger higher if you need to move more' 
                      + ' contents: moveContents(SAFETY_CAP=X) default for X is 2.')

#         bucket = self.s3.Bucket(name=from_loc)
#         print('deleting bucket: ' + str(bucket))
#         bucket.delete()
        # S3 auto-deletes empty sub-folders

        #check to see if bucket empty
        try:
            contents = self.listContents(from_bucket, from_parent, verboseTrigger=False)
            if contents == None or len(contents) == 0:
                print('{0}/{1} Successfully emptied.'.format(from_bucket,from_parent))
            else:
                print('{0}/{1} has {2} contents still in it.'.format(from_bucket,from_parent, len(contents)))             
        except:
            print('{0}/{1} Successfully emptied.'.format(from_bucket,from_parent))
                                       
        if FilesNotFound:
             print("ALERT", "Searched: {0} and found NO folder named {1}".format(from_bucket, from_parent))
        else:
            if self.explicit:
                print("dun movin " + str(fileCount) + " objects from " + from_bucket + '/' +to_bucket+ ' to ' + to_bucket +'/'+to_parent)

    def deleteContents(self, parent, bucket, fileSuffix = None, SAFETY_CAP = 2):
        loc = bucket + '/' + parent
        FilesNotFound = True
        SAFETY_TRIGGER = 0
        fileCount = 0
#         print('Bucket: ' + self.bucketrino + ' Prefix: ' + loc)
        s3_list = self.s3.Bucket(name=self.bucketrino).objects.filter(Prefix=loc)
        print(s3_list)
        for obj in s3_list:
            deleteThisOne = False
            
            if fileSuffix is None:
                deleteThisOne = True
            else: #if there's a file filter in place
                #check to see if filter matches
                if obj.key[-1*len(fileSuffix):] == fileSuffix:
                    deleteThisOne = True

            if deleteThisOne:
                print(obj.key)

                fileCount +=1
                if SAFETY_TRIGGER < SAFETY_CAP:
                    if FilesNotFound:
                        if self.explicit:
                            print("Found some objects...axin' them.")
                        FilesNotFound=False
                    #delete
                    if self.explicit:
                        print('deleting ' + str(obj.key))
                    obj.delete()
                    SAFETY_TRIGGER +=1
                else:
                    print('MORE THAN '+str(SAFETY_CAP)+' file(s)...skipping, ENABLING SAFETY TRIGGER. Set changeOutput(True) to make sure' 
                          + ' you are not deleting unintentional objects. Otherwsie, set safety trigger higher if you need to move more' 
                          + ' contents: deleteContents(SAFETY_CAP=X) default for X is 2.')

        #check to see if bucket empty
        try:
            contents = self.listContents(bucket, parent, verboseTrigger=False)
            if contents == None or len(contents) == 0:
                if self.explicit:
                    print('{0}/{1} Successfully emptied.'.format(bucket,parent))
            else:
                print('{0}/{1} has {2} contents still in it.'.format(bucket,parent, len(contents)))             
        except:
            if self.explicit:
                print('{0}/{1} Successfully emptied.'.format(bucket,parent))
                                       
        if FilesNotFound:
            if self.explicit:
                print("ALERT", "Searched: {0} and found NO folder named {1}".format(bucket, parent))
        else:
            if self.explicit:
                print("dun deletin " + str(fileCount) + " objects from " + bucket + '/' +bucket)
        

    def run_query(self, query,database=None,s3_output=None):
        if database == None:
            database = self.database
        if s3_output== None:
            s3_output = self.s3_output
#         client//athena = boto3.client('athena')
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
                }
            ,
            ResultConfiguration={
                'OutputLocation': s3_output,
                }
            )
        if self.explicit:
            print('Execution ID: ' + response['QueryExecutionId'])
        return response

    def checkQueries(self, is_queries_running,queryStore,repeatTime):
        while sum(is_queries_running) != 0: #len(is_queries_running):
            for qq in range(len(is_queries_running)):
                if is_queries_running[qq]:
                    response = self.athena.batch_get_query_execution(
                        QueryExecutionIds=[queryStore[query_num]])
                    #store full response, and just completion status
                    responses[qq] = response 
                    is_queries_running[qq] = response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED'
                    if response['QueryExecutions'][0]['Status']['State']=='FAILED':
                        print('Failed because ' +response['QueryExecutions'][0]['Status']['StateChangeReason'])
            #pause for some arbitrary amount of time
        #    print('%i queries completed out of %i /n' %query_params.sum() %len(query_params))
            #if repeat set to 0, then break out of this whole darn thing
            import time
            if repeatTime == 0:
                break
            else:
                time.sleep(repeatTime)
                self.checkQueries(is_queries_running,queryStore,repeatTime)
            if self.explicit:    
                print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries'])
            return responses    

    def fetchAppsNames(self, override=True):
        if not override:
            import os.path
            if not os.path.isfile('AppsList.csv'):
                if self.explicit:                
                    print('AppsList.csv doesnt exist -- fetching it now. \n')
                res = self.getCompanyAppsTable()
            else:
                import datetime
                if self.explicit:
                    print('AppsList.csv already exists, last updated at ' 
                      + str(datetime.datetime.fromtimestamp(os.path.getmtime('AppsList.csv'))) +
                      '. Skipping Company Apps fetch step. Change parameter for fetchAppsNames(True) '
                      'to override this and force an update. \n')
        else:
            res = self.getAppsTable()
        return res

    def getAppsTable(self,RETRY_ATTEMPTS=3):
        #probably just make this a permanent local data store
        bucketrino = 'aws-athena-query-results-XXXXXXXXXXXXXX' 
        locationrino = 'Unsaved/AppsTable'
        database = "Company"
        import boto3
        s3_output = 's3://'+bucketrino+'/'+locationrino+'/'
        queryStore =[]
        query_1 = "select id,name from Company.Apps "

        #query/ies here
        res = self.run_query(query_1,database,s3_output)
        filenamerino = res['QueryExecutionId']+'.csv'
        filerino = locationrino+'/'+filenamerino
        queryStore.append(res['QueryExecutionId'])
        print("Running Apps Table query. ")

        #pause for some arbitrary amount of time
        import time
        time.sleep(5)

        is_queries_running=[]
        response = self.athena.batch_get_query_execution(
            QueryExecutionIds=[queryStore[0]])
        #store completion status
        is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')


        if self.explicit:        
            print('Queries running?'+str(is_queries_running))
            print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' 
               + str(len(is_queries_running)) + ' queries'])

        #check and wait if running
        while (response['QueryExecutions'][0]['Status']['State']=='RUNNING'):
            is_queries_running=[]
            #can fetch layers of info about query
            response = self.athena.batch_get_query_execution(QueryExecutionIds=[queryStore[0]])
            is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')

        if self.explicit:
            print('Queries running?'+str(is_queries_running))
            print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries'])
            print('Current Status:' + response['QueryExecutions'][0]['Status']['State'])


        #If there, get it
        if response['QueryExecutions'][0]['Status']['State'] == 'SUCCEEDED':
            #check to see if query is there
            listresults = self.listContents(bucketrino,filerino)
            self.readContents(parent=locationrino,key=filenamerino,bucket=bucketrino)
            #rename file after download
            import os
            os.rename(filenamerino, 'AppsList.csv')
            if self.explicit:
                print(filenamerino+' shall now be known as.... '+'AppsList.csv.')
            return response['QueryExecutions'][0]['Status']['State']
        elif response['QueryExecutions'][0]['Status']['State'] == 'FAILED' and RETRY_ATTEMPTS>1:
            print('Event Count query failed. Pausing and will retry in 2 minutes.')
            time.sleep(120) #wait a couple minutes
            res = self.getAppsTable(self, RETRY_ATTEMPTS - 1)
            return res

        
    def importList(self, fname='AppsList.csv'):
        self.fetchAppsNames()
        
        import pandas
        import numpy as np
        apps_ids = pandas.read_csv(fname)
        apps_ids = apps_ids.as_matrix()
        apps_ids = [i[0] for i in apps_ids]
        print(np.size(apps_ids), ' elements found in list.')
        return apps_ids

    def fetchEventCount(self, override=True):
        if not override:
            import os.path
            if not os.path.isfile('EventCount.csv'):
                if self.explicit:                
                    print('EventCount.csv doesnt exist -- fetching it now. \n')
                res = self.getEventCount()
            else:
                import datetime
                if self.explicit:
                    print('EventCount.csv already exists, last updated at ' 
                      + str(datetime.datetime.fromtimestamp(os.path.getmtime('EventCount.csv'))) +
                      '. Skipping Event Count fetch step. Change parameter for fetchEventCount(True) '
                      'to override this and force an update. \n')
        else:
            res = self.getEventCount()
        return res

    def getEventCount(self, RETRY_ATTEMPTS = 3):
        #probably just make this a permanent local data store
        bucketrino = 'aws-athena-query-results-XXXXXXXXXXXXXX' 
        locationrino = 'Unsaved/AppsTable'
        database = "Company"
        import boto3
        s3_output = 's3://'+bucketrino+'/'+locationrino+'/'
        queryStore =[]
        query_1 = "select max(id) from appsresults.apps_results "

        #query/ies here
        res = self.run_query(query_1,database,s3_output)
        filenamerino = res['QueryExecutionId']+'.csv'
        filerino = locationrino+'/'+filenamerino
        queryStore.append(res['QueryExecutionId'])
        print("Running Event Count query. ")

        #pause for some arbitrary amount of time
        import time
        time.sleep(5)

        is_queries_running=[]
        response = self.athena.batch_get_query_execution(QueryExecutionIds=[queryStore[0]])
        #store completion status
        is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')


        if self.explicit:        
            print('Queries running?'+str(is_queries_running))
            print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' 
               + str(len(is_queries_running)) + ' queries'])

        time.sleep(10)
        #check and wait if running
        while (response['QueryExecutions'][0]['Status']['State']=='RUNNING'):
            is_queries_running=[]
            response = self.athena.batch_get_query_execution(QueryExecutionIds=[queryStore[0]])
            #store full response, and just completion status
            is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')

        if self.explicit:
            print('Queries running?'+str(is_queries_running))
            print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries'])
            print('Current Status:' + response['QueryExecutions'][0]['Status']['State'])
            time.sleep(10)


        #If there, get it
        if response['QueryExecutions'][0]['Status']['State'] == 'SUCCEEDED':
            #check to see if query is there
            listresults = self.listContents(bucketrino,filerino)
            self.readContents(parent=locationrino,key=filenamerino,bucket=bucketrino)
            #rename file after download
            import os
            os.rename(filenamerino, 'EventCount.csv')
            if self.explicit:
                print(filenamerino+' shall now be known as.... '+'EventCount.csv.')   
            return response['QueryExecutions'][0]['Status']['State']
        elif response['QueryExecutions'][0]['Status']['State'] == 'FAILED' and RETRY_ATTEMPTS>1:
            print('Event Count query failed. Pausing and will retry in 2 minutes.')
            time.sleep(120) #wait a couple minutes
            res = self.getEventCount(self, RETRY_ATTEMPTS - 1)
            return res

                
    def importEventCount(self, fname='EventCount.csv'):
        self.fetchEventCount()
        
        import pandas
        import numpy as np
        event_counts = pandas.read_csv(fname)
        event_counts = event_counts.as_matrix()
        event_counts = [i[0] for i in event_counts]
        print(np.size(event_counts), ' elements found in list.')
        return event_counts

    

    def fetchUserCount(self, override=True):
        if not override:
            import os.path
            if not os.path.isfile('UserCount.csv'):
                if self.explicit:                
                    print('UserCount.csv doesnt exist -- fetching it now. \n')
                res = self.getUserCount()
            else:
                import datetime
                if self.explicit:
                    print('UserCount.csv already exists, last updated at ' 
                      + str(datetime.datetime.fromtimestamp(os.path.getmtime('UserCount.csv'))) +
                      '. Skipping User Count fetch step. Change parameter for fetchUserCount(True) '
                      'to override this and force an update. \n')
        else:
            res = self.getUserCount()

    def getUserCount(self, RETRY_ATTEMPTS = 3):
        #probably just make this a permanent local data store
        bucketrino = 'aws-athena-query-results-XXXXXXXXXXXXXX' 
        locationrino = 'Unsaved/appsTable'
        database = "Company"
        import boto3
        s3_output = 's3://'+bucketrino+'/'+locationrino+'/'
        queryStore =[]
        query_1 = "select max(user_id) from appsresults.apps_results "

        #query/ies here
        res = self.run_query(query_1,database,s3_output)
        filenamerino = res['QueryExecutionId']+'.csv'
        filerino = locationrino+'/'+filenamerino
        queryStore.append(res['QueryExecutionId'])
        print("Running User Count query. ")

        #pause for some arbitrary amount of time
        import time
        time.sleep(5)

        is_queries_running=[]
        response = self.athena.batch_get_query_execution(QueryExecutionIds=[queryStore[0]])
        is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')


        if self.explicit:        
            print('Queries running?'+str(is_queries_running))
            print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' 
               + str(len(is_queries_running)) + ' queries'])

        time.sleep(10)
        #check and wait if running
        while (response['QueryExecutions'][0]['Status']['State']=='RUNNING'):
            is_queries_running=[]
            response = self.athena.batch_get_query_execution(QueryExecutionIds=[queryStore[0]])
            is_queries_running.append(response['QueryExecutions'][0]['Status']['State']!='SUCCEEDED')

        if self.explicit:
            print('Queries running?'+str(is_queries_running))
            print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries'])
            print('Current Status:' + response['QueryExecutions'][0]['Status']['State'])
            time.sleep(10)


        #If there, get it
        if response['QueryExecutions'][0]['Status']['State'] == 'SUCCEEDED':
            #check to see if query is there
            listresults = self.listContents(bucketrino,filerino)
            self.readContents(parent=locationrino,key=filenamerino,bucket=bucketrino)
            #rename file after download
            import os
            os.rename(filenamerino, 'UserCount.csv')
            if self.explicit:
                print(filenamerino+' shall now be known as.... '+'UserCount.csv.')   
            return response['QueryExecutions'][0]['Status']['State']
        elif response['QueryExecutions'][0]['Status']['State'] == 'FAILED' and RETRY_ATTEMPTS>1:
            print('User Count query failed. Pausing and will retry in 2 minutes.')
            time.sleep(120) #wait a couple minutes
            res = self.getEventCount(self, RETRY_ATTEMPTS - 1)
            return res

                
    def importUserCount(self, fname='UserCount.csv'):
        res = self.fetchUserCount()
        
        import pandas
        import numpy as np
        user_counts = pandas.read_csv(fname)
        user_counts = user_counts.as_matrix()
        user_counts = [i[0] for i in user_counts]
        print(np.size(user_counts), ' elements found in list.')
        return user_counts[0]

    

    def checkQueryStates(self,running_batch, query_state, queryStore):
        responses=[]
        is_queries_running=[]
        is_queries_failed=[]
        is_queries_finished=[]
        try:
            for query_num in range(np.size(running_batch)):
                response = self.athena.batch_get_query_execution(QueryExecutionIds=[queryStore[running_batch[query_num]]])
                #store full response, and just completion status
                query_state[running_batch[query_num]] = response['QueryExecutions'][0]['Status']['State']
                responses.append(response) 
                is_queries_running.append(response['QueryExecutions'][0]['Status']['State']=='RUNNING')
                is_queries_failed.append(response['QueryExecutions'][0]['Status']['State']=='FAILED')
                is_queries_finished.append(response['QueryExecutions'][0]['Status']['State']=='SUCCEEDED')
            if self.explicit:
                print(str(np.sum(is_queries_running)) + ' queries running, ' +str(np.sum(is_queries_failed)) + ' failed, '
                  + str(np.sum(is_queries_finished))+ ' queries finished.')
        except Exception as e: 
                print(e)
                print('Error: probably some athena spam error. waiiting for a minute.')
                time.sleep(60)
                responses,is_queries_running,is_queries_failed,is_queries_finished = self.checkQueryStates(running_batch, query_state, queryStore)
        return  responses,is_queries_running,is_queries_failed,is_queries_finished

    def generateListOfQueries(self, query_template, query_params, verboseTrigger = False):
        list_o_queries = []
        import numpy as np
        if type(query_params[0]) == int:
        #a simple list [1,2,3] vs list of lists [[1,1,1,],[2,2,2],etc
            for i in query_params:
                if verboseTrigger:
                    print(query_template.format(i))
    #         for i in range(np.size(query_params)):
                list_o_queries.append(query_template.format(i))
            
        elif type(query_params[0]) == list:            
            for i in query_params:
                if verboseTrigger:
                    print(query_template.format(*i))
    #         for i in range(np.size(query_params)):
                list_o_queries.append(query_template.format(*i))

        if self.explicit:    
            print('Sample query: \n/n')
            print(list_o_queries[0])
        return list_o_queries

    def batchQueryThroughAthena(self, list_o_queries,START_ON_QUERY,BATCH_HANDLE_LIMIT,WAIT_TIME,s3_output=None,database=None):
        import time
        if database == None:
            database = self.database
        if s3_output== None:
            s3_output = self.s3_output
        #Used for batch querying hundreds of queries and maintaing a status on all of them
        #This new version can stagger queries --> will submit new queries when other finish, even if all not finished
        running_batch = []
        completed_batch = []
        queryStore =[]
        filenamerinos=[]
        filerinos=[]
        query_submitted = [False for i in range(len(list_o_queries))]
        query_state = ['NOT STARTED' for i in range(len(list_o_queries))]

        submission_batch=list(range(START_ON_QUERY,START_ON_QUERY+BATCH_HANDLE_LIMIT))
        next_query = START_ON_QUERY + BATCH_HANDLE_LIMIT

        EMPTIES =[]
        queryStore = ['empty' for i in range(START_ON_QUERY)]

        if self.explicit:
            print('\nQUERY (THROUGH BOTO THROUGH ATHENA) REPORT: \n')
        
        while np.size(EMPTIES) < BATCH_HANDLE_LIMIT: #as long as some queries are happenin'
            if self.explicit:
                print('CURRENT QUERIES: ' + str(submission_batch))

            start = time.time()
            #submit queries
            if self.explicit:
                print('sub batc size:', len(submission_batch), ' query sub size:', len(query_submitted), ' query state size:', len(query_state))
            for i in range(np.size(submission_batch)):
                if self.explicit:
                    print("Running query: %i" %(i))
                #print(list_o_queries[batches[current_batch]])
                s3_output_a = s3_output+ 'userID_' + str(submission_batch[i]) +'/'
                res = self.run_query(list_o_queries[submission_batch[i]],database,s3_output_a)
                filenamerino = res['QueryExecutionId']+'.csv'
                filenamerinos.append(filenamerino)
                filerino = self.locationrino+'/'+filenamerino
                filerinos.append(filerino)
                queryStore.append(res['QueryExecutionId'])
                query_submitted[submission_batch[i]] = True
                #copy to running state
                running_batch.append(submission_batch[i])

            #Submitted. Clear submissions
            if self.explicit:            
                print('Submitted ' +str(np.size(submission_batch)) + ' new queries.')
            submission_batch = []

            #pause for like 5 sec to not spam
            if self.explicit:
                print('Hold up.')
            import time
            time.sleep(5)


            #store all responses for all queries    
            if self.explicit:
                print('Checking on a total of '+ str(np.size(running_batch)) + ' queries.')
            responses,is_queries_running,is_queries_failed,is_queries_finished = self.checkQueryStates(running_batch, query_state, queryStore)


            if np.sum(np.sum([i for i in is_queries_running])) ==0:
                print('Nothing running. Either an error or really fast queries.')
                sleepy_time = 0        
            else:
                if np.sum(np.sum([not i for i in is_queries_running])) ==0:
                    if self.explicit:
                        print('None done. Sleeping now.')        
                    #pause for estimated wait time
                    time.sleep(WAIT_TIME)
                    if self.explicit:
                        print('DING! SHOULD BE DONE! SURE SMELLS GOOD IN HERE!')
                        
                if self.explicit:
                    print("Checking to see if queries really finished... \n")
                responses,is_queries_running,is_queries_failed,is_queries_finished = self.checkQueryStates(running_batch, query_state, queryStore)                           
        #         print(str(np.sum([elem['QueryExecutions'][0]['Status']['State']=='RUNNING' for elem in responses])) + ' queries running.')

            #Either we skipped past errors, waited an appripriate time, some might be done, none might be done

            #update running batch/complted batch lists
            temp_tracker = []
            for rr in range(np.size(running_batch)):
                if not is_queries_running[rr]:
                    completed_batch.append(running_batch[rr])
                else:
                    temp_tracker.append(rr)
            running_batch = [running_batch[tt] for tt in temp_tracker]

            #add queries until limit
            while (np.size(EMPTIES) + np.size(submission_batch) + np.size(running_batch)) < BATCH_HANDLE_LIMIT:
                if next_query < np.size(list_o_queries):
                    submission_batch.append(next_query)
                    next_query +=1
                else:
                    EMPTIES.append('blah')
                    if self.explicit:
                        print('The end is nigh.')

        #     print('Queries running?'+str(is_queries_running))
        #     print([str(sum([not i for i in is_queries_running])) +' queries completed out of ' + str(len(is_queries_running)) + ' queries'])
        #     print('Current Status:' + str([elem['QueryExecutions'][0]['Status']['State'] for elem in responses]) + '    \n')
            if self.explicit:
                print(str(time.time()-start) + " seconds to run with "+str(np.size(temp_tracker))+" queries still running.")

        return running_batch, completed_batch, queryStore, filenamerinos, filerinos, query_submitted, query_state



#     def main():    


#         #user id table dropper
#         USER_CHUNK_SIZE = 1
#         NUM_PARTITIONS = None
#         BATCH_HANDLE_LIMIT = 4 # boto can't go over 20, Athena times out a lot with less
#         WAIT_TIME = 3*60 #60 minutes
#         EXTRA_WAIT_TIME = 3*60
#         START_ON_QUERY = 0 #should be 0

#         #staggered query submit by user_id
#         NUM_USERS = 98500000
#         USER_CHUNK_SIZE = 1000000
#         BATCH_HANDLE_LIMIT = 4 # boto can't go over 20, Athena times out a lot with less
#         WAIT_TIME = 3*60 #60 minutes
#         EXTRA_WAIT_TIME = 3*60
#         START_ON_QUERY = 0 #should be 0

#         NUM_PARTITIONS = np.size(importList)

#         import math
#         NUM_USERS = 98500000; USER_CHUNK_SIZE = 1000000; NUM_BATCHES = int(math.ceil(NUM_USERS/USER_CHUNK_SIZE)) #total_num_queries
#         #1:chunk #, 2:lower lim, 3:upper lim
#         query_params = [[i, i*USER_CHUNK_SIZE, (i+1)*USER_CHUNK_SIZE] for i in range(NUM_BATCHES)]

#         query_temp = "CREATE TABLE user{0} WITH (external_location = 's3://aws-athena-query-results-XXXXXXXXXXXXXX/AppsActivity/user_{0}/' "        "AS (SELECT id, user_id, apps_id, created_at, rank() OVER (PARTITION BY apps_id, user_id ORDER BY id) AS "        "apps_play_number, rank() OVER (PARTITION BY user_id ORDER BY id) AS master_play_number, "        "date_diff('millisecond', LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at), created_at) as master_time_last_played, "        "date_diff('millisecond', LAG(created_at) OVER (PARTITION BY user_id, apps_id ORDER BY created_at), created_at) as apps_time_last_played,  "        "date_diff('millisecond', MIN(created_at) OVER (PARTITION BY user_id ORDER BY created_at), created_at) as master_time_first_played, "        "date_diff('millisecond', MIN(created_at) OVER (PARTITION BY user_id, apps_id ORDER BY created_at), created_at) as apps_time_first_played "        "FROM appsresults.apps_results WHERE (user_id>={1} AND user_id<{2})"

#         #Staggered user partitioned batch table dropper
#         query_temp = "DROP TABLE appsresults.user{0}" 

#         #apps id partitioning --> maybe a CDAB
#         query_temp = "CREATE TABLE apps_id_{0} WITH (external_location = "        "'s3://aws-athena-query-results-XXXXXXXXXXXXXX/apps_activity_partitioned/apps_id_{0}/') AS "        "(SELECT * FROM appsresults.uga WHERE (apps_id={0}) ORDER BY user_id, created_at )"] ## axe this line if run time different
#         apps_ids = importList()
#         query_params = [[i] in apps_ids]

#         query_temp = "DROP TABLE appsresults.apps_id_{0}" 

#         # client = boto3.client('athena', region_name='us-east-1')
#         list_o_queries = generateListOfQueries(query_temp, query_params)

#         import time
#         super_start = time.time()
#         #running_batch, completed_batch, queryStore, filenamerinos, filerinos, query_submitted, query_state
#         batchQueryThroughAthena(list_o_queries, START_ON_QUERY,BATCH_HANDLE_LIMIT,WAIT_TIME,s3_output,database,client)
#         print(str((time.time()-super_start)/3600) + " hours to run "+str(np.size(list_o_queries))+" queries.")

