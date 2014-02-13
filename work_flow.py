class basic_function:
    def checksum_is_ok(old_file, new_file):
        try:
            check_sum the file before upload.
            upload file.
            check_sum the file after upload.
            if old_check_sum == new_check_sum:
                return 1
            return 0
        except:
            raise error

    def upload_video(self):
        read static_db => choose 2 most-free nodes
        for each node:
            upload to this node.
            if checksum_is_ok => save self to mongodb:
                {film_id: xxx;
                 url: "/fbox/film_tinh_cam/hellomoto.mp4"
                 main_node: node01;
                 back_up_node: node02;
                 status: 1}

    def remove_is_ok(self):
        if not os.is_file(self.url):
            return 1
        return 0

    def hard_remove(self):
        for node in self.on_node:
            remove from nodes.
            if remove_is_ok: continue
            else: return "cannot remove file from node xxx"
        return save to mongodb


    def move_video(location1, location2):
        try:
            os.popen('rsync location1 location2')
            retry = 0
            if exist_status != 0 and retry < config['MAX_RETRY']:
                do retry
                retry += 1
        except:
            raise error

        if check_sum_is_ok:
            query mongodb => get old_film
            remove file from location1
            if remove_is_ok(location1) then update to mongodb(location1 => location2)
            else: 
                print "cannot remove old file, try to roll back"
                try remove file from location2
                except: raise "cannot remove new file, roll back fail"
                if remove_is_ok(location2): print "roll back successfully"


    def swap_video(vid_A,vid_B):
        try:
            move_video(vid_A from node_of_A to node_of_B)
        except:
            raise error

        try:
            move_video(vid_B from node_of_B to node_of_A)
        except:
            try:
               move_video(vid_A from node_of_A to node_of_B) ## (roll_back)
            except :
                raise "cannot roll back", error
            raise error, "success roll back"



class controller_fuction:

    def relocate_file():
        '''This function runs daily or weekly, it determines the traffic utilized on each server, then relocate the hotest files to the sparest files'''
        




