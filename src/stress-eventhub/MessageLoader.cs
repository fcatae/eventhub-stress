using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;

namespace stress_eventhub
{
    public class MessageLoader
    {
        List<string> _messages = new List<string>();
        Random _random = new Random(0);

        public int MessageCount
        {
            get { return _messages.Count; }
        }

        public void Load(string path)
        {
            List<string> messages = new List<string>();

            DirectoryInfo currentFolder = new DirectoryInfo(path);
            var files = currentFolder.EnumerateFiles("*.msg");

            foreach(var file in files)
            {
                string body = ReadFile(file);

                messages.Add(body);
            }

            this._messages = messages;
        }

        string ReadFile(FileInfo file)
        {
            using(var reader = file.OpenText())
            {
                string body = reader.ReadToEnd();
                return body;
            }
        }

        public string GetRandom()
        {
            if (_messages.Count == 0)
                return "No message found";

            int random_number = _random.Next( _messages.Count );

            return _messages[random_number];
        }
    }
}
