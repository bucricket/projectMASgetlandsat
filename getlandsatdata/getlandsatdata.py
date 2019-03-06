#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 19 08:51:51 2017

@author: mschull
"""
import os
import numpy as np
import glob
import shutil
import pandas as pd
from datetime import datetime
import wget
import argparse
import getpass
import keyring
import json
import pycurl
import requests
from time import sleep
import logging
import tarfile
import gzip
import zipfile

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

host = 'https://espa.cr.usgs.gov/api/v1/'
TIMEOUT = 86400


def espa_api(endpoint, verb='get', body=None, uauth=None):
    """ Suggested simple way to interact with the ESPA JSON REST API """
    #    auth_tup = uauth if uauth else print "need USGS creds!" exit()
    if uauth:
        auth_tup = uauth
    else:
        print("need USGS creds!")
        exit()

    response = getattr(requests, verb)(host + endpoint, auth=auth_tup, json=body)
    print('{} {}'.format(response.status_code, response.reason))
    data = response.json()
    if isinstance(data, dict):
        messages = data.pop("messages", None)
        if messages:
            print(json.dumps(messages, indent=4))
    try:
        response.raise_for_status()
    except Exception as e:
        print(e)
        return None
    else:
        return data


def extract_archive(source_path, destination_path=None, delete_originals=False):
    """
    Attempts to decompress the following formats for input filepath
    Support formats include `.tar.gz`, `.tar`, `.gz`, `.zip`.
    :param source_path:         a file path to an archive
    :param destination_path:    path to unzip, will be same name with dropped extension if left None
    :param delete_originals:    Set to "True" if archives may be deleted after
                                their contents is successful extracted.
    """

    head, tail = os.path.split(source_path)

    def set_destpath(destpath, file_ext):
        if destpath is not None:
            return destpath
        else:
            return os.path.join(head, tail.replace(file_ext, ""))

    if source_path.endswith(".tar.gz"):
        with tarfile.open(source_path, 'r:gz') as tfile:
            tfile.extractall(set_destpath(destination_path, ".tar.gz"))
            ret = destination_path

    # gzip only compresses single files
    elif source_path.endswith(".gz"):
        with gzip.open(source_path, 'rb') as gzfile:
            content = gzfile.read()
            with open(set_destpath(destination_path, ".gz"), 'wb') as of:
                of.write(content)
            ret = destination_path

    elif source_path.endswith(".tar"):
        with tarfile.open(source_path, 'r') as tfile:
            tfile.extractall(set_destpath(destination_path, ".tar"))
            ret = destination_path

    elif source_path.endswith(".zip"):
        with zipfile.ZipFile(source_path, "r") as zipf:
            zipf.extractall(set_destpath(destination_path, ".zip"))
            ret = destination_path

    else:
        raise Exception("supported types are tar.gz, gz, tar, zip")

    print("Extracted {0}".format(source_path))
    if delete_originals:
        os.remove(source_path)

    return ret


class BaseDownloader(object):
    """ basic downloader class with general/universal download utils """

    def __init__(self, local_dir):
        self.local_dir = local_dir
        self.queue = []

        if not os.path.exists(local_dir):
            os.mkdir(local_dir)

    @staticmethod
    def _download(source, dest, retries=2):
        trynum = 0
        while trynum < retries:
            try:
                wget.download(url=source, out=dest)
                return dest
            except:
                sleep(1)

    @staticmethod
    def _extract(source, dest):
        """ extracts a file to destination"""
        return extract_archive(source, dest, delete_originals=False)

    def _raw_destination_mapper(self, source):
        """ returns raw download destination from source url"""
        filename = os.path.basename(source)
        return os.path.join(self.local_dir, filename)

    def _ext_destination_mapper(self, source):
        """ maps a raw destination into an extracted directory dest """
        filename = os.path.basename(source).replace(".tar.gz", "")
        tilename = filename
        return os.path.join(self.local_dir, tilename)

    def download(self, source, mode='w', cleanup=True):
        """
        Downloads the source url and extracts it to a folder. Returns
        a tuple with the extract destination, and a bool to indicate if it is a
        fresh download or if it was already found at that location.

        :param source:  url from which to download data
        :param mode:    either 'w' or 'w+' to write or overwrite
        :param cleanup: use True to delete intermediate files (the tar.gz's)
        :return: tuple(destination path (str), new_download? (bool))
        """
        raw_dest = self._raw_destination_mapper(source)
        ext_dest = self._ext_destination_mapper(raw_dest)
        if not os.path.exists(ext_dest) or mode == 'w+':
            self._download(source, raw_dest)
            self._extract(raw_dest, ext_dest)
            fresh = True
        else:
            print("Found: {0}, Use mode='w+' to force rewrite".format(ext_dest))
            fresh = False
        if cleanup and os.path.exists(raw_dest):
            os.remove(raw_dest)
        return ext_dest, fresh


def check_order_cache(auth):
    # This program checks the last month of orders from ESPA
    username = auth[0]
    password = auth[1]

    def api_request(endpoint, verb='get', json=None, uauth=None):
        """
        Here we can see how easy it is to handle calls to a REST API that uses JSON
        """
        auth_tup = uauth if uauth else (username, password)
        response = getattr(requests, verb)(host + endpoint, auth=auth_tup, json=json)
        return response.json()

    def espa_api(endpoint, verb='get', body=None, uauth=None):
        """ Suggested simple way to interact with the ESPA JSON REST API """
        auth_tup = uauth if uauth else (username, password)
        response = getattr(requests, verb)(host + endpoint, auth=auth_tup, json=body)
        print('{} {}'.format(response.status_code, response.reason))
        data = response.json()
        if isinstance(data, dict):
            messages = data.pop("messages", None)
            if messages:
                print(json.dumps(messages, indent=4))
        try:
            response.raise_for_status()
        except Exception as e:
            print(e)
            return None
        else:
            return data

    #    usr = api_request('user')

    #    order_list = api_request('list-orders/%s' % usr['email'])
    filters = {"status": ["complete", "ordered"]}  # Here, we ignore any purged orders
    order_list = espa_api('list-orders', body=filters)
    orderID = []
    fName = []
    order_status = []
    #    for i in range(len(order_list['orders'])):
    #        orderid = order_list['orders'][i]
    for i in range(len(order_list)):
        orderid = order_list[i]
        resp = espa_api('item-status/{0}'.format(orderid))
        ddd = json.loads(json.dumps(resp))
        #        if not ddd['orderid']['%s' % orderid][0]['status']=='purged':
        for j in range(len(ddd['%s' % orderid])):
            fname = ddd['%s' % orderid][j]['name']
            status = ddd['%s' % orderid][j]['status']
            orderID.append(orderid)
            fName.append(fname)
            order_status.append(status)

    output = {'orderid': orderID, 'productID': fName, 'status': order_status}
    outDF = pd.DataFrame(output)

    return outDF


def search(lat, lon, start_date, end_date, cloud, cacheDir, sat):
    columns = ['acquisitionDate', 'acquisitionDate', 'upperLeftCornerLatitude', 'upperLeftCornerLongitude',
               'lowerRightCornerLatitude', 'lowerRightCornerLongitude', 'cloudCover', 'sensor', 'LANDSAT_PRODUCT_ID']
    end = datetime.strptime(end_date, '%Y-%m-%d')
    # this is a landsat-util work around when it fails
    if sat == 7:
        metadataUrl = 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_ETM_C1.csv'
    else:
        metadataUrl = 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_8_C1.csv'

    fn = os.path.join(cacheDir, metadataUrl.split(os.sep)[-1])
    # looking to see if metadata CSV is available and if its up to the date needed
    if os.path.exists(fn):
        d = datetime.fromtimestamp(os.path.getmtime(fn))
        if (end.year > d.year) and (end.month > d.month) and (end.day > d.day):
            wget.download(metadataUrl, out=fn)
        df = pd.read_csv(fn, usecols=columns)
        index = ((df.acquisitionDate >= start_date) & (df.acquisitionDate < end_date) & (
                df.upperLeftCornerLatitude > lat) & (df.upperLeftCornerLongitude < lon) & (
                         df.lowerRightCornerLatitude < lat) & (df.lowerRightCornerLongitude > lon) & (
                         df.cloudCover <= cloud) & (df.sensor == 'OLI_TIRS'))
        df = df[index]

    else:
        wget.download(metadataUrl, out=fn)
        df = pd.read_csv(fn, usecols=columns)
        index = ((df.acquisitionDate >= start_date) & (df.acquisitionDate < end_date) & (
                df.upperLeftCornerLatitude > lat) & (df.upperLeftCornerLongitude < lon) & (
                         df.lowerRightCornerLatitude < lat) & (df.lowerRightCornerLongitude > lon) & (
                         df.cloudCover <= cloud) & (df.sensor == 'OLI_TIRS'))
        df = df[index]

    return df


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


def find_already_downloaded(df, cache_dir):
    usgs_available = list(df.LANDSAT_PRODUCT_ID.values)
    # find sat
    sat = usgs_available[0].split("_")[0][-1]
    # find scenes
    scenes = [x.split("_")[2] for x in usgs_available]
    scenes = list(set(scenes))
    available_list = []
    for scene in scenes:
        available = [os.path.basename(x) for x in
                     glob.glob(os.path.join(cache_dir,'L%s/%s/RAW_DATA/*MTL*' % (sat, scene )))]
        available = [x[:-8] for x in available]
        available_list = available_list + available
    return intersection(usgs_available, available_list)


def find_not_downloaded(df, cache_dir):
    usgs_available = list(df.LANDSAT_PRODUCT_ID.values)
    # find sat
    sat = usgs_available[0].split("_")[0][-1]
    # find scenes
    scenes = [x.split("_")[2] for x in usgs_available]
    scenes = list(set(scenes))
    available_list = []
    for scene in scenes:
        available = [os.path.basename(x) for x in
                     glob.glob(os.path.join(cache_dir,'L%s/%s/RAW_DATA/*MTL*' % (sat, scene )))]
        available = [x[:-8] for x in available]
        available_list = available_list + available
    for x in available_list:
        usgs_available.remove(x)

    return usgs_available


def download_order_gen(order_id, auth, downloader=None, sleep_time=300, timeout=86400, **dlkwargs):
    """
    This function is a generator that yields the results from the input downloader classes
    download() method. This is a generator mostly so that data pipeline functions that operate
    upon freshly downloaded files may immediately get started on them.

    :param order_id:            order name
    :param downloader:          optional downloader for tiles. child of BaseDownloader class
                                of a Downloaders.BaseDownloader or child class
    :param sleep_time:          number of seconds to wait between checking order status
    :param timeout:             maximum number of seconds to run program
    :param dlkwargs:            keyword arguments for downloader.download() method.
    :returns:                   yields values from the input downloader.download() method.
    """

    complete = False
    reached_timeout = False
    starttime = datetime.now()

    if downloader is None:
        downloader = BaseDownloader('espa_downloads')

    while not complete and not reached_timeout:
        # wait a while before the next ping and check timeout condition
        elapsed_time = (datetime.now() - starttime).seconds
        reached_timeout = elapsed_time > timeout
        print("Elapsed time is {0}m".format(elapsed_time / 60.0))

        # check order completion status, and list all items which ARE complete
        #            complete_items = self._complete_items(order_id, verbose=False)

        filters = {"status": "complete"}  # Here, we ignore any purged orders
        complete_items = espa_api('item-status/{0}'.format(order_id), uauth=auth, body=filters)[order_id]
        for c in complete_items:
            if isinstance(c, dict):
                url = c["product_dload_url"]
            elif isinstance(c, requests.Request):
                url = c.json()["product_dload_url"]
            else:
                raise Exception("Could not interpret {0}".format(c))
            yield downloader.download(url, **dlkwargs)
        resp = espa_api('item-status/{0}'.format(order_id), uauth=auth)
        all_items = resp[order_id]

        active_items = [item for item in all_items
                        if item['status'] != 'complete' and
                        item['status'] != 'error' and
                        item['status'] != 'unavailable' and
                        item['status'] != 'purged']

        complete = (len(active_items) < 1)
        if not complete:
            sleep(sleep_time)


def get_landsat_data(sceneIDs, auth):
    username = auth[0]
    password = auth[1]

    #    client = Client(auth)
    def api_request(endpoint, verb='get', json=None, uauth=None):
        """
        Here we can see how easy it is to handle calls to a REST API that uses JSON
        """
        auth_tup = uauth if uauth else (username, password)
        response = getattr(requests, verb)(host + endpoint, auth=auth_tup, json=json)
        return response.json()

    # =====set products=======
    l8_prods = ['sr', 'bt']
    # =====search for data=======
    print("Searching...")
    ordered_data = check_order_cache(auth)
    l8_tiles = []
    orderedIDs_completed = []
    orderedIDs_not_completed = []
    sceneIDs_completed = []
    sceneIDs_not_completed = []
    for sceneID in sceneIDs:
        if not ordered_data.empty:
            if np.sum(ordered_data.productID == sceneID) > 0:
                completed_test = (ordered_data.productID == sceneID) & (ordered_data.status == 'complete')
                not_complete_test = (ordered_data.productID == sceneID) & (ordered_data.status != 'complete')
                if len(ordered_data[completed_test]) > 0:
                    orderedIDs_completed.append(list(ordered_data.orderid[completed_test])[0])
                    sceneIDs_completed.append(sceneID)
                else:
                    orderedIDs_not_completed.append(list(ordered_data.orderid[not_complete_test])[0])
                    sceneIDs_not_completed.append(sceneID)
            else:
                l8_tiles.append(sceneID)
        else:
            l8_tiles.append(sceneID)

    if l8_tiles:
        print("Ordering new data...")
        # ========setup order=========
        order = espa_api('available-products', uauth=auth, body=dict(inputs=l8_tiles))
        for sensor in order.keys():
            if isinstance(order[sensor], dict) and order[sensor].get('inputs'):
                order[sensor]['products'] = l8_prods

        order['format'] = 'gtiff'
        # =======order the data============
        resp = espa_api('order', verb='post', uauth=auth, body=order)
        print(json.dumps(resp, indent=4))
        orderidNew = resp['orderid']

    if orderedIDs_completed:

        print("downloading completed existing orders...")
        print(orderedIDs_completed)
        i = -1
        for orderid in orderedIDs_completed:
            i += 1
            sceneID = sceneIDs_completed[i]
            complete = False
            reached_TIMEOUT = False
            starttime = datetime.now()
            while not complete and not reached_TIMEOUT:
                resp = espa_api('item-status/{0}'.format(orderid), uauth=auth)
                for item in resp[orderid]:
                    if item.get('name') == sceneID:
                        url = item.get('product_dload_url')
                        elapsed_time = (datetime.now() - starttime).seconds
                        reached_TIMEOUT = elapsed_time > TIMEOUT
                        print("Elapsed time is {0}m".format(elapsed_time / 60.0))
                        if len(url) > 0:
                            downloader = BaseDownloader('espa_downloads')
                            downloader.download(url)
                            # if os.path.exists(os.path.join(os.getcwd,'espa_downloads',url.split(os.sep)[-1][:-7])):
                            complete = True

                        if not complete:
                            sleep(300)

    if orderedIDs_not_completed:
        print("waiting for cached existing orders...")
        i = -1
        for orderid in orderedIDs_not_completed:
            i += 1
            complete = False
            reached_TIMEOUT = False
            starttime = datetime.now()
            sceneID = sceneIDs_not_completed[i]
            while not complete and not reached_TIMEOUT:
                resp = api_request('item-status/{0}'.format(orderid))
                for item in resp[orderid]:
                    if item.get('name') == sceneID:
                        url = item.get('product_dload_url')
                        elapsed_time = (datetime.now() - starttime).seconds
                        reached_TIMEOUT = elapsed_time > TIMEOUT
                        print("Elapsed time is {0}m".format(elapsed_time / 60.0))
                        if len(url) > 0:
                            downloader = BaseDownloader('espa_downloads')
                            downloader.download(url)
                            # if os.path.exists(os.path.join(os.getcwd,'espa_downloads',url.split(os.sep)[-1][:-7])):
                            complete = True

                        if not complete:
                            sleep(300)

    if l8_tiles:
        print("Download new data...")
        # ======Download data=========
        for download in download_order_gen(orderidNew, auth):
            print(download)


def main():
    # Get time and location from user
    parser = argparse.ArgumentParser()
    parser.add_argument("lat", type=float, help="latitude")
    parser.add_argument("lon", type=float, help="longitude")
    parser.add_argument("start_date", type=str, help="Start date yyyy-mm-dd")
    parser.add_argument("end_date", type=str, help="Start date yyyy-mm-dd")
    parser.add_argument("cloud", type=int, help="cloud coverage")
    parser.add_argument("orderOrsearch", type=str, help="type 'order' for order and 'search'"
                                                        "for print search results or 'update' to update the database with existing data")
    parser.add_argument('-s', '--sat', nargs='?', type=int, default=8,
                        help='which landsat to search or download, i.e. Landsat 8 = 8')
    parser.add_argument('-f', '--find', nargs='*', type=str, default=None,
                        help='top directory to search for local files to be added to the main cache')
    args = parser.parse_args()

    loc = [args.lat, args.lon]
    start_date = args.start_date
    end_date = args.end_date
    cloud = args.cloud
    orderOrsearch = args.orderOrsearch
    sat = args.sat
    cacheDir = os.path.abspath(os.path.join(os.getcwd(), "SATELLITE_DATA", "LANDSAT"))
    if not os.path.exists(cacheDir):
        os.makedirs(cacheDir)

    # =====USGS credentials===============
    # need to get this from pop up
    usgs_user = str(getpass.getpass(prompt="usgs username:"))
    if keyring.get_password("usgs", usgs_user) is None:
        usgs_pass = str(getpass.getpass(prompt="usgs password:"))
        keyring.set_password("usgs", usgs_user, usgs_pass)
    else:
        usgs_pass = str(keyring.get_password("usgs", usgs_user))

        # ======search for landsat data not on system===============================
    if orderOrsearch == 'search':
        output_df = search(loc[0], loc[1], start_date, end_date, cloud, cacheDir, sat)
        print("====data needed to be downloaded==============================")
        print(find_not_downloaded(output_df, cacheDir))
        print("====data available on system==================================")
        print(find_already_downloaded(output_df, cacheDir))

    elif orderOrsearch == 'update':
        findDir = args.find
        findDir = findDir[0]

        # ====find all landsat files on system==================================
        fns = []
        paths = []
        for root, dirs, files in os.walk(findDir):
            for file in files:
                if (file.startswith("LC08")) and (file.endswith("MTL.txt")):
                    fns.append(os.path.join(root, file))
                    paths.append(root)
        i = 0
        productIDs = []
        for fn in fns:
            print(fn)
            path = paths[i]
            print(path)
            i += 1
            productIDs.append('_'.join(fn.split(os.sep)[-1].split('_')[:7]))
        # =========copy all landsat files to the cache and put cache location in the database
        for productID in productIDs:
            for path in paths:
                fns = glob.glob(os.path.join(path, "*%s*" % productID))
                scene = productID.split(os.sep)[-1].split('_')[2]
                folder = os.path.join(cacheDir, "L%d" % sat, scene, "RAW_DATA")
                if len(fns) > 0:
                    for filename in fns:
                        fn = filename.split(os.sep)[-1]
                        outfn = os.path.join(folder, fn)
                        if not os.path.exists(outfn):
                            print("copying: %s " % productID)
                            shutil.copy(filename, folder)
                    continue

    else:
        output_df = search(loc[0], loc[1], start_date, end_date, cloud, cacheDir, sat)

        productIDs = find_not_downloaded(output_df,cacheDir)

        # start Landsat order process
        get_landsat_data(productIDs, ("%s" % usgs_user, "%s" % usgs_pass))
        # ========move surface relectance files=====================================
        download_folder = os.path.join(os.getcwd(), 'espa_downloads')
        folders_2move = glob.glob(os.path.join(download_folder, '*'))
        i = 0
        paths = []
        for folder_2move in folders_2move:
            scene = folder_2move.split(os.sep)[-1].split('-')[0][4:10]
            folder = os.path.join(cacheDir, "L%d" % sat, scene, "RAW_DATA")
            if not os.path.exists(folder):
                os.makedirs(folder)

            for filename in glob.glob(os.path.join(folder_2move, '*.*')):
                shutil.copy(filename, folder)
            paths.append(folder)

        if len(folders_2move) > 0:
            # ======Clean up folder===============================
            shutil.rmtree(download_folder)

        print("All done downloading data!!")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, pycurl.error):
        exit('Received Ctrl + C... Exiting! Bye.', 1)
