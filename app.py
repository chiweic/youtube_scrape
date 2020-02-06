from youtube_transcript_api import YouTubeTranscriptApi
import logging
import json
import codecs
from pytube import YouTube
from pymongo import MongoClient


def progress_callback(stream, chunk, file_handle, bytes_remaining):
    logging.info('bytes_remaining {}'.format(bytes_remaining))

def download_audio(video_id, output_path, filename):
    yt_url = 'https://www.youtube.com/watch?v=' + video_id
    yt = YouTube(yt_url)
    audio_stream = yt.streams.filter(only_audio=True).first()
    # return the full path of the audio
    return audio_stream.download(output_path=output_path, filename=filename)

def extract_caption(video_id, language):
    # get transcripts
    return YouTubeTranscriptApi.get_transcript(video_id=video_id, languages=[language])


def update_db(collection, video_id, lang, category):
    # extract caption
    logging.info('extracting transcription')
    trans = extract_caption(video_id=video_id, language=lang)

    # download audio
    logging.info('download audio')
    audio_path= download_audio(video_id=video_id,
                               output_path='/home/chiweic/audio_src', filename=video_id)

    # index by video_id
    collection.find_one_and_update({'video_id':video_id},
                                   {'$set':{'trans': trans, 'audio': audio_path, 'category': category}},
                                   upsert=True)


# main routine
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # access to mongodb
    col = MongoClient(host='gpu-server').get_database('audio').get_collection('youtube')
    videos = json.load(codecs.open('tianxia.json', 'r', encoding='utf-8'))
    for video in videos:
        logging.info('update {}'.format(video))
        update_db(col, video['video_id'], video['language'], video['category'])

