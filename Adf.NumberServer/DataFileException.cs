using System;

namespace Adf.NumberServer
{
    class DataFileException : Exception
    {
        public DataFileException(string message)
            : base(message) { }


        public DataFileException(Exception innerException, string message)
            : base(message, innerException) { }
    }
}
