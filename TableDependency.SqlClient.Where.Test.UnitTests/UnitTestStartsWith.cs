using System;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using TableDependency.SqlClient.Where.Test.UnitTests.Models;

namespace TableDependency.SqlClient.Where.Test.UnitTests
{
    [TestClass]
    public class UnitTestStartsWith
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