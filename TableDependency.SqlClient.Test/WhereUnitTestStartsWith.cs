using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Test.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test
{
    [TestClass]
    public class WhereUnitTestStartsWith
    {
        [TestMethod]
        public void StartsWith1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.StartsWith("123");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[Code] LIKE '123%'", where);
        }

        [TestMethod]
        public void EndsWith1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.EndsWith("123");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("[Code] LIKE '%123'", where);
        }

        [TestMethod]
        public void StartsAndEndsWith1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.EndsWith("123") && p.Code.StartsWith("123");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Code] LIKE '%123' AND [Code] LIKE '123%')", where);
        }

        [TestMethod]
        public void StartsAndEndsWith2()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => (p.Code.EndsWith("123")) && (p.Code.StartsWith("123"));

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Code] LIKE '%123' AND [Code] LIKE '123%')", where);
        }

        [TestMethod]
        public void StartsOrEndsWith1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.EndsWith("123") || p.Code.StartsWith("123");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Code] LIKE '%123' OR [Code] LIKE '123%')", where);
        }

        [TestMethod]
        public void EndsOrStartsWith2()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.StartsWith("123") || p.Code.EndsWith("123");

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("([Code] LIKE '123%' OR [Code] LIKE '%123')", where);
        }
    }
}