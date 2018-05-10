using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TableDependency.SqlClient.Where.Tests.Models;

namespace TableDependency.SqlClient.Where.Tests
{
    [Table("TrimTest")]
    public class TrimTest
    {
        public string Name { get; set; }

        [Column("Long Description")]
        public string Description { get; set; }
    }

    [TestClass]
    public class UnitTestTrim
    {
        [TestMethod]
        public void Trim1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.Trim() == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(LTRIM(RTRIM([Code])) = '123')", where);
        }

        [TestMethod]
        public void LTrim1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.TrimEnd() == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(RTRIM([Code]) = '123')", where);
        }

        [TestMethod]
        public void RTrim1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.TrimStart() == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("(LTRIM([Code]) = '123')", where);
        }

        [TestMethod]
        public void LRTrim1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.TrimStart() == "123" && p.Code.TrimEnd() == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("((LTRIM([Code]) = '123') AND (RTRIM([Code]) = '123'))", where);
        }

        [TestMethod]
        public void RLTrim1()
        {
            // Arrange
            Expression<Func<Product, bool>> expression = p => p.Code.TrimEnd() == "123" && p.Code.TrimStart() == "123";

            // Act
            var where = new SqlTableDependencyFilter<Product>(expression).Translate();

            // Assert
            Assert.AreEqual("((RTRIM([Code]) = '123') AND (LTRIM([Code]) = '123'))", where);
        }        
    }
}