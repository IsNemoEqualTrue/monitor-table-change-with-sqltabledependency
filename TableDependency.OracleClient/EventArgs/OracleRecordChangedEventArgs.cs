////////////////////////////////////////////////////////////////////////////////
//   TableDependency, SqlTableDependency, OracleTableDependency
//   Copyright (c) Christian Del Bianco.  All rights reserved.
////////////////////////////////////////////////////////////////////////////////
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Xml.Linq;
using TableDependency.Enums;
using TableDependency.EventArgs;
using TableDependency.Mappers;
using TableDependency.OracleClient.MessageTypes;

namespace TableDependency.OracleClient.EventArgs
{
    public sealed class OracleRecordChangedEventArgs<T> : RecordChangedEventArgs<T> where T : class
    {
        #region Member variables

        private static readonly IEnumerable<PropertyInfo> _entiyProperiesInfo;

        private static readonly IList<Type> _types = new List<Type>()
        {
            typeof (string),
            typeof (short), typeof (short?),
            typeof (int), typeof (int?),
            typeof (long), typeof (long?),       
            typeof (decimal), typeof (decimal?),
            typeof (float), typeof (float?),
            typeof (DateTime), typeof (DateTime?),
            typeof (double), typeof (double?),
            typeof (bool), typeof (bool?)
        };

        #endregion

        #region Properties

        /// <summary>
        /// Gets modified record.
        /// </summary>
        /// <value>
        /// The changed entity.
        /// </value>
        public override T Entity { get; protected set; }

        /// <summary>
        /// Gets the change type (insert/delete/update).
        /// </summary>
        /// <value>
        /// The change action.
        /// </value>
        public override ChangeType ChangeType { get; protected set; }

        /// <summary>
        /// Gets SQL message type.
        /// </summary>
        /// <value>
        /// The type of the message.
        /// </value>
        public override string MessageType { get; protected set; }

        #endregion

        #region Constructors

        static OracleRecordChangedEventArgs()
        {
            _entiyProperiesInfo = typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.SetField)
                .Where(propertyInfo => _types.Contains(propertyInfo.PropertyType) || (propertyInfo.PropertyType.IsGenericType && propertyInfo.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                .ToArray();
        }

        internal OracleRecordChangedEventArgs(string messageType, string message, ModelToTableMapper<T> mapper)
        {
            Entity = MaterializeEntity(message, mapper);
            ChangeType = SetChangeType(messageType);
        }

        #endregion

        #region Private methods

        private static ChangeType SetChangeType(string messageType)
        {
            switch (messageType)
            {
                case CustomMessageTypes.DeletedMessageType:
                    return ChangeType.Delete;
                case CustomMessageTypes.InsertedMessageType:
                    return ChangeType.Insert;
                case CustomMessageTypes.UpdatedMessageType:
                    return ChangeType.Update;
            }

            return ChangeType.None;
        }

        private static T MaterializeEntity(string stringXmlDocument, ModelToTableMapper<T> mapper)
        {
            if (string.IsNullOrWhiteSpace(stringXmlDocument)) return default(T);

            var xDocument = XDocument.Parse(stringXmlDocument);
            if (xDocument.Root == null) return default(T);

            var xColumns = xDocument.Root.Elements("column").ToList();         
            var entity = (T)Activator.CreateInstance(typeof(T));

            foreach (var entityPropertyInfo in _entiyProperiesInfo)
            {
                var propertyMappedTo = mapper?.GetMapping(entityPropertyInfo);
                var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

                var xmlNode = xColumns.FirstOrDefault(x => string.Compare(x.Attribute("name").Value, propertyName, StringComparison.OrdinalIgnoreCase) == 0);
                if (xmlNode != default(XElement))
                {
                    var columnValue = xmlNode.Value.Trim();
                    var value = TypeDescriptor.GetConverter(entityPropertyInfo.PropertyType).ConvertFromString(columnValue);
                    entityPropertyInfo.SetValue(entity, value);                    
                }
            }

            return entity;
        }

        #endregion
    }
}