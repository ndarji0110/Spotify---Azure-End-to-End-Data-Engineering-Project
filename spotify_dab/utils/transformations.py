class reusable :

    def dropcolumns(self,df,columns):
        df = df.drop(*columns)
        return df
    

    