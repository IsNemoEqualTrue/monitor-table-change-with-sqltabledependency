using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.UnitTests.Models;

namespace TableDependency.SqlClient.Where.UnitTests
{
    [TestClass]
    public class UnitTestParameterString
    {
        [TestMethod]
        public void UnitTestParameters1()
        {
            var par1 = "WWW";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).EndsWith(par1);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE '%WWW'", where);
        }

        [TestMethod]
        public void UnitTestParameters2()
        {
            var par1 = "WWW";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).Contains(par1);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE '%WWW%'", where);
        }

        [TestMethod]
        public void UnitTestParameters3()
        {
            var par1 = "WWW";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).StartsWith(par1);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE 'WWW%'", where);
        }

        [TestMethod]
        public void UnitTestParameters4()
        {
            var par1 = "WWW";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).Equals(par1);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) = 'WWW'", where);
        }

        [TestMethod]
        public void UnitTestParameters5()
        {
            var par1 = "WWW";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3) == par1;

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) = 'WWW')", where);
        }

        [TestMethod]
        public void UnitTestParameters6()
        {
            var par1 = "WWW";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToLower().Substring(0, 3) == par1.ToLower();

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(SUBSTRING(LOWER(LTRIM(RTRIM([Code]))), 0, 3) = LOWER('WWW'))", where);
        }

        [TestMethod]
        public void UnitTestParameters7()
        {
            var par1 = "WWW";

            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToLower().Substring(0, 3) == par1.Trim().ToLower().Substring(0, 3);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(SUBSTRING(LOWER(LTRIM(RTRIM([Code]))), 0, 3) = SUBSTRING(LOWER(LTRIM(RTRIM('WWW'))), 0, 3))", where);
        }
    }
}