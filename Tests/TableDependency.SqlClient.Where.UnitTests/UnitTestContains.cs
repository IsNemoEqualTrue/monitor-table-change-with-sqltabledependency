using System;
using System.Linq;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.UnitTests.Models;

namespace TableDependency.SqlClient.Where.UnitTests
{
    [TestClass]
    public class UnitTestContains
    {
        [TestMethod]
        public void StringContains1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Contains("123");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[Code] LIKE '%123%'", where);
        }

        [TestMethod]
        public void StringContains2()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Contains("123");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[Code] LIKE '%123%'", where);
        }

        [TestMethod]
        public void ColumnContainsNumbers()
        {
            var ids = new[] { 1, 2, 3 };

            // Arrange
            Expression<Func<Product, bool>> expression = p => ids.Contains(p.Id);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[Id] IN (1,2,3)", where);
        }

        [TestMethod]
        public void ColumnContainsStrings()
        {
            var codes = new[] { "one", "two" };

            // Arrange
            Expression<Func<Product, bool>> expression = p => codes.Contains(p.Code);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[Code] IN ('one','two')", where);
        }

        [TestMethod]
        public void ColumnContainsDecimals()
        {
            var prices = new[] { 123.45M, 432.10M };

            // Arrange
            Expression<Func<Product, bool>> expression = p => prices.Contains(p.Price);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[Price] IN (123.45,432.10)", where);
        }

        [TestMethod]
        public void ColumnContainsFloats()
        {
            var prices = new[] { 123.45f, 432.10f };

            // Arrange
            Expression<Func<Product, bool>> expression = p => prices.Contains(p.ExcangeRate);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[ExcangeRate] IN (123.45,432.1)", where);
        }

        [TestMethod]
        public void ColumnContainsDates()
        {
            var codes = new[] {
                DateTime.ParseExact("2010-05-18 14:40:52,531", "yyyy-MM-dd HH:mm:ss,fff", System.Globalization.CultureInfo.InvariantCulture),
                DateTime.ParseExact("2009-05-18 14:40:52,531", "yyyy-MM-dd HH:mm:ss,fff", System.Globalization.CultureInfo.InvariantCulture)
            };

            // Arrange
            Expression<Func<Product, bool>> expression = p => codes.Contains(p.ExpireDateTime);

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[ExpireDateTime] IN ('2010-05-18T14:40:52','2009-05-18T14:40:52')", where);
        }
    }
}