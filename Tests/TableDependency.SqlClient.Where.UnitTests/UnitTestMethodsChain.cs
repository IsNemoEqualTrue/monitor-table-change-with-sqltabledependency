using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.UnitTests.Models;

namespace TableDependency.SqlClient.Where.UnitTests
{
    [TestClass]
    public class UnitTestMethodsChain
    {
        [TestMethod]
        public void MethodsChain1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).EndsWith("WWW");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE '%WWW'", where);
        }

        [TestMethod]
        public void MethodsChain2()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).Contains("WWW");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE '%WWW%'", where);
        }
    }
}