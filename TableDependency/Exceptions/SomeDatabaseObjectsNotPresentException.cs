////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.Text;

namespace TableDependency.Exceptions
{
    [Serializable]
    public class SomeDatabaseObjectsNotPresentException : TableDependencyException
    {
        protected internal SomeDatabaseObjectsNotPresentException(Dictionary<string, bool> missingObjects)
            : base()
        {
            var errorMessage = new StringBuilder("Having specified a naming convention for Database objects all of them must be absents or be presents. The following object are missing:");
            foreach (var missingObject in missingObjects)
            {
                if (missingObject.Value == false)
                {
                    errorMessage.AppendLine(missingObject.Key + " ");
                }
            }            
        }
    }
}